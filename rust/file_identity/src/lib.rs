#![deny(unsafe_op_in_unsafe_fn)]

use std::{
    ffi::OsStr,
    fs::File,
    io,
    path::{Path, PathBuf},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Identity {
    pub first: u64,
    pub second: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileVersion {
    pub identity: Identity,
    pub size: u64,
    pub modified: i128,
    pub changed: i128,
}

pub struct DirectoryHandle {
    path: PathBuf,
    file: File,
    identity: Identity,
    ancestors: Vec<(File, Identity)>,
}

fn validate_entry_name(name: &OsStr) -> io::Result<()> {
    let entry = Path::new(name);
    if entry.file_name() != Some(name) || entry.components().count() != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "directory entry must be a single name",
        ));
    }
    Ok(())
}

fn stable_absolute_path(path: &Path) -> io::Result<PathBuf> {
    if path.is_absolute() {
        return Ok(path.to_owned());
    }
    Ok(std::env::current_dir()?.join(path))
}

impl DirectoryHandle {
    pub fn identity(&self) -> Identity {
        self.identity
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn same_identity(&self) -> io::Result<bool> {
        if identity(&self.file)? != self.identity {
            return Ok(false);
        }
        let current = open_directory_chain(&self.path, false)?;
        if current.identity != self.identity {
            return Ok(false);
        }
        for (file, expected) in &self.ancestors {
            if identity(file)? != *expected {
                return Ok(false);
            }
        }
        Ok(true)
    }

    #[cfg(unix)]
    fn file(&self) -> &File {
        &self.file
    }
}

fn verify_replace_target_binding(
    directory: &DirectoryHandle,
    target_name: &OsStr,
    replace_if_exists: bool,
    target_file: Option<&File>,
) -> io::Result<()> {
    match (replace_if_exists, target_file) {
        (false, None) => Ok(()),
        (true, Some(expected)) => {
            let expected_identity = identity(expected)?;
            let current = open_regular_file_for_replace_check_at(directory, target_name)?;
            if identity(&current)? != expected_identity {
                return Err(io::Error::other(
                    "replacement target changed after it was opened",
                ));
            }
            Ok(())
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "replace flag and target handle must describe the same target state",
        )),
    }
}

#[cfg(unix)]
pub fn identity(file: &File) -> io::Result<Identity> {
    use std::os::unix::fs::MetadataExt;

    let metadata = file.metadata()?;
    Ok(Identity {
        first: metadata.dev(),
        second: metadata.ino(),
    })
}

#[cfg(unix)]
pub fn version(file: &File) -> io::Result<FileVersion> {
    use std::os::unix::fs::MetadataExt;

    let metadata = file.metadata()?;
    Ok(FileVersion {
        identity: Identity {
            first: metadata.dev(),
            second: metadata.ino(),
        },
        size: metadata.len(),
        modified: i128::from(metadata.mtime()) * 1_000_000_000 + i128::from(metadata.mtime_nsec()),
        changed: i128::from(metadata.ctime()) * 1_000_000_000 + i128::from(metadata.ctime_nsec()),
    })
}

#[cfg(windows)]
#[allow(unsafe_code)]
pub fn identity(file: &File) -> io::Result<Identity> {
    use std::{mem::MaybeUninit, os::windows::io::AsRawHandle};
    use windows_sys::Win32::{
        Foundation::HANDLE,
        Storage::FileSystem::{
            BY_HANDLE_FILE_INFORMATION, FILE_ATTRIBUTE_REPARSE_POINT, GetFileInformationByHandle,
        },
    };

    let mut information = MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::zeroed();
    // SAFETY: the handle is borrowed from a live File and the Windows API
    // writes exactly one BY_HANDLE_FILE_INFORMATION value to the pointer.
    let success = unsafe {
        GetFileInformationByHandle(file.as_raw_handle() as HANDLE, information.as_mut_ptr())
    };
    if success == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: GetFileInformationByHandle returned nonzero after initializing
    // the output structure.
    let information = unsafe { information.assume_init() };
    if information.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "reparse point is not an approved regular file or directory",
        ));
    }
    Ok(Identity {
        first: u64::from(information.dwVolumeSerialNumber),
        second: (u64::from(information.nFileIndexHigh) << 32)
            | u64::from(information.nFileIndexLow),
    })
}

#[cfg(windows)]
#[allow(unsafe_code)]
pub fn version(file: &File) -> io::Result<FileVersion> {
    use std::{mem::MaybeUninit, os::windows::io::AsRawHandle};
    use windows_sys::Win32::{
        Foundation::HANDLE,
        Storage::FileSystem::{FILE_BASIC_INFO, FileBasicInfo, GetFileInformationByHandleEx},
    };

    let identity = identity(file)?;
    let mut information = MaybeUninit::<FILE_BASIC_INFO>::zeroed();
    // SAFETY: the handle is borrowed from a live File and the API writes one
    // FILE_BASIC_INFO value into the correctly sized output buffer.
    let success = unsafe {
        GetFileInformationByHandleEx(
            file.as_raw_handle() as HANDLE,
            FileBasicInfo,
            information.as_mut_ptr().cast(),
            u32::try_from(std::mem::size_of::<FILE_BASIC_INFO>()).expect("FILE_BASIC_INFO size"),
        )
    };
    if success == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: GetFileInformationByHandleEx returned nonzero after filling the
    // output structure.
    let information = unsafe { information.assume_init() };
    Ok(FileVersion {
        identity,
        size: file.metadata()?.len(),
        modified: i128::from(information.LastWriteTime),
        changed: i128::from(information.ChangeTime),
    })
}

#[cfg(unix)]
fn nix_error(error: nix::Error) -> io::Error {
    io::Error::from_raw_os_error(error as i32)
}

#[cfg(unix)]
fn open_directory_chain(path: &Path, create_missing: bool) -> io::Result<DirectoryHandle> {
    use nix::{
        fcntl::{OFlag, open},
        sys::stat::{Mode, mkdirat},
    };

    let path = stable_absolute_path(path)?;
    let anchor = if path.is_absolute() {
        Path::new("/")
    } else {
        Path::new(".")
    };
    let fd = open(
        anchor,
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .map_err(nix_error)?;
    let mut current = File::from(fd);
    let mut ancestors = Vec::new();
    for component in path.components() {
        let name = match component {
            std::path::Component::RootDir | std::path::Component::CurDir => continue,
            std::path::Component::Normal(name) => name,
            std::path::Component::ParentDir | std::path::Component::Prefix(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "directory path contains an unsupported component",
                ));
            }
        };
        let next = match nix::fcntl::openat(
            &current,
            name,
            OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
            Mode::empty(),
        ) {
            Ok(fd) => fd,
            Err(error) if create_missing && error == nix::Error::ENOENT => {
                mkdirat(&current, name, Mode::from_bits_truncate(0o700)).map_err(nix_error)?;
                nix::fcntl::openat(
                    &current,
                    name,
                    OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
                    Mode::empty(),
                )
                .map_err(nix_error)?
            }
            Err(error) => return Err(nix_error(error)),
        };
        let current_identity = identity(&current)?;
        ancestors.push((current, current_identity));
        current = File::from(next);
    }
    let identity = identity(&current)?;
    Ok(DirectoryHandle {
        path,
        identity,
        file: current,
        ancestors,
    })
}

#[cfg(unix)]
pub fn open_directory(path: &Path) -> io::Result<DirectoryHandle> {
    open_directory_chain(path, false)
}

#[cfg(unix)]
pub fn open_or_create_directory(path: &Path) -> io::Result<DirectoryHandle> {
    open_directory_chain(path, true)
}

#[cfg(windows)]
pub fn directory_identity_at(parent: &DirectoryHandle, name: &OsStr) -> io::Result<Identity> {
    use std::os::windows::fs::MetadataExt;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_REPARSE_POINT, FILE_READ_ATTRIBUTES,
        FILE_TRAVERSE, OPEN_EXISTING,
    };

    validate_entry_name(name)?;
    if !parent.same_identity()? {
        return Err(io::Error::other("directory parent changed"));
    }
    let path = entry_path(parent, name)?;
    let child = open_with_delete_share(
        &path,
        FILE_READ_ATTRIBUTES | FILE_TRAVERSE,
        OPEN_EXISTING,
        true,
    )?;
    let metadata = child.metadata()?;
    if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
        || metadata.file_attributes() & FILE_ATTRIBUTE_DIRECTORY == 0
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "path is not an approved directory",
        ));
    }
    let identity = identity(&child)?;
    if !parent.same_identity()? {
        return Err(io::Error::other("directory parent changed"));
    }
    Ok(identity)
}

#[cfg(unix)]
pub fn directory_identity_at(parent: &DirectoryHandle, name: &OsStr) -> io::Result<Identity> {
    validate_entry_name(name)?;
    if !parent.same_identity()? {
        return Err(io::Error::other("directory parent changed"));
    }
    let fd = nix::fcntl::openat(
        parent.file(),
        name,
        nix::fcntl::OFlag::O_RDONLY
            | nix::fcntl::OFlag::O_DIRECTORY
            | nix::fcntl::OFlag::O_NOFOLLOW
            | nix::fcntl::OFlag::O_CLOEXEC,
        nix::sys::stat::Mode::empty(),
    )
    .map_err(nix_error)?;
    let child = File::from(fd);
    if !parent.same_identity()? {
        return Err(io::Error::other("directory parent changed"));
    }
    identity(&child)
}

#[cfg(unix)]
pub fn remove_directory_at(
    parent: &DirectoryHandle,
    name: &OsStr,
    expected_identity: Identity,
) -> io::Result<()> {
    use nix::fcntl::{OFlag, openat};
    use nix::sys::stat::Mode;

    validate_entry_name(name)?;
    if !parent.same_identity()? {
        return Err(io::Error::other("directory parent changed"));
    }
    let fd = openat(
        parent.file(),
        name,
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .map_err(nix_error)?;
    let child = File::from(fd);
    if identity(&child)? != expected_identity {
        return Err(io::Error::other("directory entry changed"));
    }
    if !parent.same_identity()? {
        return Err(io::Error::other("directory parent changed"));
    }
    nix::unistd::unlinkat(parent.file(), name, nix::unistd::UnlinkatFlags::RemoveDir)
        .map_err(nix_error)
}

#[cfg(unix)]
pub fn open_regular_file_at(directory: &DirectoryHandle, name: &OsStr) -> io::Result<File> {
    use nix::{
        fcntl::{OFlag, openat},
        sys::stat::Mode,
    };

    validate_entry_name(name)?;
    let fd = openat(
        directory.file(),
        name,
        OFlag::O_RDONLY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .map_err(nix_error)?;
    let file = File::from(fd);
    if !file.metadata()?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "path is not an approved regular file",
        ));
    }
    identity(&file)?;
    Ok(file)
}

#[cfg(unix)]
pub fn open_regular_file_for_delete_at(
    directory: &DirectoryHandle,
    name: &OsStr,
) -> io::Result<File> {
    open_regular_file_at(directory, name)
}

#[cfg(unix)]
pub fn open_regular_file_for_replace_at(
    directory: &DirectoryHandle,
    name: &OsStr,
) -> io::Result<File> {
    open_regular_file_at(directory, name)
}

#[cfg(unix)]
pub fn open_regular_file_for_replace_check_at(
    directory: &DirectoryHandle,
    name: &OsStr,
) -> io::Result<File> {
    open_regular_file_at(directory, name)
}

#[cfg(unix)]
pub fn open_regular_file_read_write_at(
    directory: &DirectoryHandle,
    name: &OsStr,
) -> io::Result<File> {
    use nix::{
        fcntl::{OFlag, openat},
        sys::stat::Mode,
    };

    validate_entry_name(name)?;
    let fd = openat(
        directory.file(),
        name,
        OFlag::O_RDWR | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .map_err(nix_error)?;
    let file = File::from(fd);
    if !file.metadata()?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "path is not an approved regular file",
        ));
    }
    identity(&file)?;
    Ok(file)
}

#[cfg(unix)]
pub fn create_regular_file_at(directory: &DirectoryHandle, name: &OsStr) -> io::Result<File> {
    use nix::{
        fcntl::{OFlag, openat},
        sys::stat::Mode,
    };

    validate_entry_name(name)?;
    let fd = openat(
        directory.file(),
        name,
        OFlag::O_WRONLY | OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
        Mode::from_bits_truncate(0o600),
    )
    .map_err(nix_error)?;
    let file = File::from(fd);
    if !file.metadata()?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "path is not an approved regular file",
        ));
    }
    identity(&file)?;
    Ok(file)
}

#[cfg(unix)]
pub fn open_regular_file_for_lock_at(
    directory: &DirectoryHandle,
    name: &OsStr,
) -> io::Result<File> {
    open_regular_file_read_write_at(directory, name)
}

#[cfg(unix)]
pub fn create_regular_file_for_lock_at(
    directory: &DirectoryHandle,
    name: &OsStr,
) -> io::Result<File> {
    create_regular_file_at(directory, name)
}

#[cfg(unix)]
pub fn remove_file_at(directory: &DirectoryHandle, name: &OsStr) -> io::Result<()> {
    validate_entry_name(name)?;
    nix::unistd::unlinkat(
        directory.file(),
        name,
        nix::unistd::UnlinkatFlags::NoRemoveDir,
    )
    .map_err(nix_error)
}

#[cfg(unix)]
pub fn remove_open_file_at(
    directory: &DirectoryHandle,
    name: &OsStr,
    file: &File,
) -> io::Result<()> {
    validate_entry_name(name)?;
    if !directory.same_identity()? {
        return Err(io::Error::other("directory parent changed"));
    }
    identity(file)?;
    if !directory.same_identity()? {
        return Err(io::Error::other("directory parent changed"));
    }
    nix::unistd::unlinkat(
        directory.file(),
        name,
        nix::unistd::UnlinkatFlags::NoRemoveDir,
    )
    .map_err(nix_error)
}

#[cfg(unix)]
pub fn remove_open_file_at_bound(
    directory: &DirectoryHandle,
    name: &OsStr,
    file: &File,
) -> io::Result<()> {
    use nix::fcntl::{OFlag, openat};
    use nix::sys::stat::Mode;

    validate_entry_name(name)?;
    if identity(directory.file())? != directory.identity {
        return Err(io::Error::other("directory handle changed"));
    }
    let expected = identity(file)?;
    let current = File::from(
        openat(
            directory.file(),
            name,
            OFlag::O_RDONLY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
            Mode::empty(),
        )
        .map_err(nix_error)?,
    );
    if !current.metadata()?.is_file() || identity(&current)? != expected {
        return Err(io::Error::other("directory entry changed"));
    }
    if identity(directory.file())? != directory.identity {
        return Err(io::Error::other("directory handle changed"));
    }
    nix::unistd::unlinkat(
        directory.file(),
        name,
        nix::unistd::UnlinkatFlags::NoRemoveDir,
    )
    .map_err(nix_error)
}

#[cfg(unix)]
pub fn replace_file_at(
    directory: &DirectoryHandle,
    temporary_name: &OsStr,
    target_name: &OsStr,
    _temporary_file: &File,
    _replace_if_exists: bool,
    _target_file: Option<&File>,
) -> io::Result<()> {
    validate_entry_name(temporary_name)?;
    validate_entry_name(target_name)?;
    verify_replace_target_binding(directory, target_name, _replace_if_exists, _target_file)?;

    #[cfg(target_os = "linux")]
    if !_replace_if_exists {
        use std::{ffi::CString, os::unix::ffi::OsStrExt, os::unix::io::AsRawFd};

        let temporary_name = CString::new(temporary_name.as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "file name contains NUL"))?;
        let target_name = CString::new(target_name.as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "file name contains NUL"))?;
        // renameat2(RENAME_NOREPLACE) is the atomic no-overwrite primitive;
        // a pre-opened target check alone would leave a create race.
        let result = unsafe {
            libc::syscall(
                libc::SYS_renameat2,
                directory.file().as_raw_fd(),
                temporary_name.as_ptr(),
                directory.file().as_raw_fd(),
                target_name.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
        return Ok(());
    }

    #[cfg(not(target_os = "linux"))]
    if !_replace_if_exists {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "atomic no-replace rename is unavailable on this Unix target",
        ));
    }

    nix::fcntl::renameat(
        directory.file(),
        temporary_name,
        directory.file(),
        target_name,
    )
    .map_err(nix_error)
}

#[cfg(windows)]
fn windows_api_path_wide(path: &Path) -> Vec<u16> {
    use std::os::windows::ffi::OsStrExt;

    let raw = path.as_os_str().encode_wide().collect::<Vec<_>>();
    let is_device_path = raw.starts_with(&[b'\\' as u16, b'\\' as u16, b'?' as u16, b'\\' as u16])
        || raw.starts_with(&[b'\\' as u16, b'\\' as u16, b'.' as u16, b'\\' as u16]);
    if !path.is_absolute() || raw.len() < 248 || is_device_path {
        return raw;
    }
    if raw.starts_with(&[b'\\' as u16, b'\\' as u16]) {
        "\\\\?\\UNC\\"
            .encode_utf16()
            .chain(raw[2..].iter().copied())
            .collect()
    } else {
        "\\\\?\\"
            .encode_utf16()
            .chain(raw.iter().copied())
            .collect()
    }
}

#[cfg(windows)]
#[allow(unsafe_code)]
fn open_with_flags(
    path: &Path,
    desired_access: u32,
    creation: u32,
    directory: bool,
) -> io::Result<File> {
    open_with_sharing(path, desired_access, creation, directory, false)
}

#[cfg(windows)]
#[allow(unsafe_code)]
fn open_with_delete_share(
    path: &Path,
    desired_access: u32,
    creation: u32,
    directory: bool,
) -> io::Result<File> {
    open_with_sharing(path, desired_access, creation, directory, true)
}

#[cfg(windows)]
#[allow(unsafe_code)]
fn open_with_sharing(
    path: &Path,
    desired_access: u32,
    creation: u32,
    directory: bool,
    share_delete: bool,
) -> io::Result<File> {
    use std::{os::windows::io::FromRawHandle, ptr};
    use windows_sys::Win32::{
        Foundation::{HANDLE, INVALID_HANDLE_VALUE},
        Storage::FileSystem::{
            CreateFileW, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ,
            FILE_SHARE_WRITE,
        },
    };

    let mut wide = windows_api_path_wide(path);
    wide.push(0);
    let flags = FILE_FLAG_OPEN_REPARSE_POINT
        | if directory {
            FILE_FLAG_BACKUP_SEMANTICS
        } else {
            0
        };
    // SAFETY: wide is NUL-terminated for the duration of the call; the
    // remaining pointers are null by contract and the returned handle is
    // transferred exactly once into File below.
    let handle = unsafe {
        CreateFileW(
            wide.as_ptr(),
            desired_access,
            FILE_SHARE_READ
                | FILE_SHARE_WRITE
                | if share_delete {
                    windows_sys::Win32::Storage::FileSystem::FILE_SHARE_DELETE
                } else {
                    0
                },
            ptr::null(),
            creation,
            flags,
            0 as HANDLE,
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: handle was returned by CreateFileW and is now owned by File.
    Ok(unsafe { File::from_raw_handle(handle) })
}

#[cfg(windows)]
fn directory_prefixes(path: &Path) -> Vec<PathBuf> {
    let mut prefixes = path
        .ancestors()
        .filter(|prefix| !prefix.as_os_str().is_empty())
        .map(Path::to_owned)
        .collect::<Vec<_>>();
    prefixes.reverse();
    if !path.is_absolute() {
        prefixes.insert(0, PathBuf::from("."));
    }
    prefixes
}

#[cfg(windows)]
fn open_directory_chain(path: &Path, create_missing: bool) -> io::Result<DirectoryHandle> {
    use std::os::windows::fs::MetadataExt;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_REPARSE_POINT, FILE_READ_ATTRIBUTES,
        FILE_TRAVERSE, OPEN_EXISTING,
    };

    let path = stable_absolute_path(path)?;
    let mut current = None;
    let mut ancestors = Vec::new();
    let prefixes = directory_prefixes(&path);
    for (index, prefix) in prefixes.iter().enumerate() {
        let desired_access = if index + 1 == prefixes.len() {
            FILE_READ_ATTRIBUTES | FILE_TRAVERSE
        } else {
            FILE_READ_ATTRIBUTES
        };
        let file = match open_with_flags(prefix, desired_access, OPEN_EXISTING, true) {
            Ok(file) => file,
            Err(error) if create_missing && error.kind() == io::ErrorKind::NotFound => {
                std::fs::create_dir(prefix)?;
                open_with_flags(prefix, desired_access, OPEN_EXISTING, true)?
            }
            Err(error) => return Err(error),
        };
        let metadata = file.metadata()?;
        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
            || metadata.file_attributes() & FILE_ATTRIBUTE_DIRECTORY == 0
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "path is not an approved directory",
            ));
        }
        if let Some(previous) = current.replace(file) {
            let previous_identity = identity(&previous)?;
            ancestors.push((previous, previous_identity));
        }
    }
    let file = current
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "empty directory path"))?;
    let identity = identity(&file)?;
    Ok(DirectoryHandle {
        path: path.to_owned(),
        file,
        identity,
        ancestors,
    })
}

#[cfg(windows)]
pub fn open_directory(path: &Path) -> io::Result<DirectoryHandle> {
    open_directory_chain(path, false)
}

#[cfg(windows)]
pub fn open_or_create_directory(path: &Path) -> io::Result<DirectoryHandle> {
    open_directory_chain(path, true)
}

#[cfg(windows)]
pub fn remove_directory_at(
    parent: &DirectoryHandle,
    name: &OsStr,
    expected_identity: Identity,
) -> io::Result<()> {
    use std::os::windows::fs::MetadataExt;
    use windows_sys::Win32::Storage::FileSystem::{
        DELETE, FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_REPARSE_POINT, FILE_READ_ATTRIBUTES,
        FILE_TRAVERSE, OPEN_EXISTING,
    };

    validate_entry_name(name)?;
    if !parent.same_identity()? {
        return Err(io::Error::other("directory parent changed"));
    }
    let path = entry_path(parent, name)?;
    let child = open_with_delete_share(
        &path,
        DELETE | FILE_READ_ATTRIBUTES | FILE_TRAVERSE,
        OPEN_EXISTING,
        true,
    )?;
    let metadata = child.metadata()?;
    if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
        || metadata.file_attributes() & FILE_ATTRIBUTE_DIRECTORY == 0
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "path is not an approved directory",
        ));
    }
    if identity(&child)? != expected_identity {
        return Err(io::Error::other("directory entry changed"));
    }
    if !parent.same_identity()? {
        return Err(io::Error::other("directory parent changed"));
    }
    dispose_file_by_handle(&child)
}

#[cfg(windows)]
fn entry_path(directory: &DirectoryHandle, name: &OsStr) -> io::Result<PathBuf> {
    validate_entry_name(name)?;
    Ok(directory.path.join(name))
}

#[cfg(windows)]
fn validate_regular_file(file: File) -> io::Result<File> {
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "path is not an approved regular file",
        ));
    }
    identity(&file)?;
    Ok(file)
}

#[cfg(windows)]
pub fn open_regular_file_at(directory: &DirectoryHandle, name: &OsStr) -> io::Result<File> {
    use windows_sys::Win32::Storage::FileSystem::{FILE_GENERIC_READ, OPEN_EXISTING};

    let path = entry_path(directory, name)?;
    let file = open_with_flags(&path, FILE_GENERIC_READ, OPEN_EXISTING, false)?;
    validate_regular_file(file)
}

#[cfg(windows)]
pub fn open_regular_file_for_delete_at(
    directory: &DirectoryHandle,
    name: &OsStr,
) -> io::Result<File> {
    use windows_sys::Win32::Storage::FileSystem::{
        DELETE, FILE_READ_ATTRIBUTES, FILE_READ_DATA, OPEN_EXISTING,
    };

    let path = entry_path(directory, name)?;
    let file = open_with_delete_share(
        &path,
        DELETE | FILE_READ_DATA | FILE_READ_ATTRIBUTES,
        OPEN_EXISTING,
        false,
    )?;
    validate_regular_file(file)
}

#[cfg(windows)]
pub fn open_regular_file_for_replace_at(
    directory: &DirectoryHandle,
    name: &OsStr,
) -> io::Result<File> {
    use windows_sys::Win32::Storage::FileSystem::{
        DELETE, FILE_READ_ATTRIBUTES, FILE_READ_DATA, OPEN_EXISTING,
    };

    let path = entry_path(directory, name)?;
    let file = open_with_delete_share(
        &path,
        DELETE | FILE_READ_DATA | FILE_READ_ATTRIBUTES,
        OPEN_EXISTING,
        false,
    )?;
    validate_regular_file(file)
}

#[cfg(windows)]
pub fn open_regular_file_for_replace_check_at(
    directory: &DirectoryHandle,
    name: &OsStr,
) -> io::Result<File> {
    use windows_sys::Win32::Storage::FileSystem::{FILE_GENERIC_READ, OPEN_EXISTING};

    let path = entry_path(directory, name)?;
    let file = open_with_delete_share(&path, FILE_GENERIC_READ, OPEN_EXISTING, false)?;
    validate_regular_file(file)
}

#[cfg(windows)]
pub fn open_regular_file_read_write_at(
    directory: &DirectoryHandle,
    name: &OsStr,
) -> io::Result<File> {
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_GENERIC_READ, FILE_GENERIC_WRITE, OPEN_EXISTING,
    };

    let path = entry_path(directory, name)?;
    let file = open_with_flags(
        &path,
        FILE_GENERIC_READ | FILE_GENERIC_WRITE,
        OPEN_EXISTING,
        false,
    )?;
    validate_regular_file(file)
}

#[cfg(windows)]
pub fn open_regular_file_for_lock_at(
    directory: &DirectoryHandle,
    name: &OsStr,
) -> io::Result<File> {
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_GENERIC_READ, FILE_GENERIC_WRITE, OPEN_EXISTING,
    };

    let path = entry_path(directory, name)?;
    let file = open_with_flags(
        &path,
        FILE_GENERIC_READ | FILE_GENERIC_WRITE,
        OPEN_EXISTING,
        false,
    )?;
    validate_regular_file(file)
}

#[cfg(windows)]
pub fn create_regular_file_at(directory: &DirectoryHandle, name: &OsStr) -> io::Result<File> {
    use windows_sys::Win32::Storage::FileSystem::{
        CREATE_NEW, DELETE, FILE_GENERIC_READ, FILE_GENERIC_WRITE,
    };

    let path = entry_path(directory, name)?;
    let file = open_with_delete_share(
        &path,
        FILE_GENERIC_READ | FILE_GENERIC_WRITE | DELETE,
        CREATE_NEW,
        false,
    )?;
    validate_regular_file(file)
}

#[cfg(windows)]
pub fn create_regular_file_for_lock_at(
    directory: &DirectoryHandle,
    name: &OsStr,
) -> io::Result<File> {
    use windows_sys::Win32::Storage::FileSystem::{
        CREATE_NEW, FILE_GENERIC_READ, FILE_GENERIC_WRITE,
    };

    let path = entry_path(directory, name)?;
    let file = open_with_flags(
        &path,
        FILE_GENERIC_READ | FILE_GENERIC_WRITE,
        CREATE_NEW,
        false,
    )?;
    validate_regular_file(file)
}

#[cfg(windows)]
#[allow(unsafe_code)]
fn dispose_file_by_handle(file: &File) -> io::Result<()> {
    use std::{mem::size_of, os::windows::io::AsRawHandle};
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_DISPOSITION_FLAG_DELETE, FILE_DISPOSITION_FLAG_IGNORE_READONLY_ATTRIBUTE,
        FILE_DISPOSITION_FLAG_POSIX_SEMANTICS, FILE_DISPOSITION_INFO_EX, FileDispositionInfoEx,
        SetFileInformationByHandle,
    };

    let information = FILE_DISPOSITION_INFO_EX {
        Flags: FILE_DISPOSITION_FLAG_DELETE
            | FILE_DISPOSITION_FLAG_IGNORE_READONLY_ATTRIBUTE
            | FILE_DISPOSITION_FLAG_POSIX_SEMANTICS,
    };
    // SAFETY: the handle is borrowed from a live File and information points
    // to one initialized FILE_DISPOSITION_INFO_EX value for the call.
    if unsafe {
        SetFileInformationByHandle(
            file.as_raw_handle() as windows_sys::Win32::Foundation::HANDLE,
            FileDispositionInfoEx,
            (&information as *const FILE_DISPOSITION_INFO_EX).cast(),
            size_of::<FILE_DISPOSITION_INFO_EX>() as u32,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(all(test, unix))]
mod unix_tests {
    use super::*;
    use std::{
        ffi::OsStr,
        fs,
        io::Write,
        path::{Path, PathBuf},
        time::{SystemTime, UNIX_EPOCH},
    };

    fn test_root(label: &str) -> PathBuf {
        let base = std::env::var_os("CHATGPT2API_TEST_TMPDIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .parent()
                    .and_then(Path::parent)
                    .expect("project root above file_identity manifest")
                    .join(".local")
                    .join("codex")
                    .join("tmp")
            });
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        base.join(format!(
            "file-identity-{label}-{}-{stamp}",
            std::process::id()
        ))
    }

    #[test]
    fn bound_remove_follows_original_directory_after_parent_rebind() {
        let root = test_root("parent-rebind");
        let parent = root.join("parent");
        let moved = root.join("parent-moved");
        let name = OsStr::new("owned.bin");
        fs::create_dir_all(&parent).expect("parent");
        let directory = open_directory(&parent).expect("directory handle");
        let mut created = create_regular_file_at(&directory, name).expect("owned file");
        created.write_all(b"owned").expect("owned bytes");
        created.sync_all().expect("owned sync");
        drop(created);
        let retained = open_regular_file_for_replace_at(&directory, name).expect("retained");

        fs::rename(&parent, &moved).expect("rebind parent");
        fs::create_dir(&parent).expect("replacement parent");
        fs::write(parent.join(name), b"external").expect("external file");

        remove_open_file_at_bound(&directory, name, &retained).expect("bound removal");
        assert_eq!(
            fs::read(parent.join(name)).expect("external survives"),
            b"external"
        );
        assert!(
            !moved.join(name).exists(),
            "owned entry remains in old parent"
        );

        drop(retained);
        fs::remove_file(parent.join(name)).expect("replacement file cleanup");
        fs::remove_dir(&parent).expect("replacement parent cleanup");
        fs::remove_dir(&moved).expect("old parent cleanup");
        fs::remove_dir(&root).expect("root cleanup");
    }

    #[test]
    fn bound_remove_rejects_same_name_replacement() {
        let root = test_root("entry-replacement");
        let parent = root.join("parent");
        let name = OsStr::new("owned.bin");
        fs::create_dir_all(&parent).expect("parent");
        let directory = open_directory(&parent).expect("directory handle");
        let mut created = create_regular_file_at(&directory, name).expect("owned file");
        created.write_all(b"owned").expect("owned bytes");
        created.sync_all().expect("owned sync");
        drop(created);
        let retained = open_regular_file_for_replace_at(&directory, name).expect("retained");

        fs::remove_file(parent.join(name)).expect("replace original");
        fs::write(parent.join(name), b"attacker").expect("replacement");
        assert!(
            remove_open_file_at_bound(&directory, name, &retained).is_err(),
            "replacement must not be removed"
        );
        assert_eq!(
            fs::read(parent.join(name)).expect("replacement survives"),
            b"attacker"
        );

        drop(retained);
        fs::remove_dir_all(&root).expect("root cleanup");
    }
}

#[cfg(all(test, windows))]
mod tests {
    use super::*;
    use std::{
        collections::BTreeSet,
        ffi::OsStr,
        fs,
        io::{Read, Write},
        os::windows::ffi::OsStrExt,
        path::{Path, PathBuf},
        time::{SystemTime, UNIX_EPOCH},
    };
    use windows_sys::Win32::Storage::FileSystem::FILE_RENAME_INFO;

    struct TestDirectory {
        path: PathBuf,
    }

    impl TestDirectory {
        fn new(label: &str) -> Self {
            let base = std::env::var_os("CHATGPT2API_TEST_TMPDIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| {
                    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                        .parent()
                        .and_then(Path::parent)
                        .expect("project root above file_identity manifest")
                        .join(".local")
                        .join("codex")
                        .join("tmp")
                        .join("rust")
                });
            let root = base.join("file-identity").join(format!(
                "{label}-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("clock")
                    .as_nanos()
            ));
            fs::create_dir_all(&root).expect("file identity test directory");
            Self { path: root }
        }

        fn child(&self, name: &str) -> PathBuf {
            let path = self.path.join(name);
            fs::create_dir_all(&path).expect("file identity case directory");
            path
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn assert_replace_case(
        parent: &Path,
        target_name: &OsStr,
        temporary_name: &OsStr,
        existing_target: Option<&[u8]>,
    ) {
        let target_path = parent.join(target_name);
        if let Some(bytes) = existing_target {
            fs::write(&target_path, bytes).expect("existing target");
        }
        let directory = open_directory(parent).expect("verified parent directory");
        let target_file = existing_target
            .map(|_| open_regular_file_for_replace_at(&directory, target_name).expect("target"));
        let mut temporary_file =
            create_regular_file_at(&directory, temporary_name).expect("temporary file");
        let expected = b"new payload\n";
        temporary_file
            .write_all(expected)
            .expect("temporary contents");
        temporary_file.sync_all().expect("temporary flush");
        let temporary_identity = identity(&temporary_file).expect("temporary identity");

        replace_file_at(
            &directory,
            temporary_name,
            target_name,
            &temporary_file,
            existing_target.is_some(),
            target_file.as_ref(),
        )
        .expect("atomic replace");
        drop(temporary_file);
        drop(target_file);

        let entries = fs::read_dir(parent)
            .expect("enumerate parent")
            .map(|entry| {
                entry
                    .expect("directory entry")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect::<BTreeSet<_>>();
        let target_text = target_name.to_string_lossy().into_owned();
        assert_eq!(entries, BTreeSet::from([target_text.clone()]));
        let mut target = open_regular_file_at(&directory, target_name).expect("published target");
        let mut contents = Vec::new();
        target
            .read_to_end(&mut contents)
            .expect("published contents");
        assert_eq!(contents, expected);
        assert_eq!(
            identity(&target).expect("published identity"),
            temporary_identity
        );
        assert!(
            !parent.join(temporary_name).exists(),
            "temporary entry remains"
        );
        assert!(parent.join(target_name).exists(), "target entry is missing");
    }

    #[test]
    fn windows_rename_api_matrix_records_each_contract() {
        use std::{mem::size_of, os::windows::io::AsRawHandle};
        use windows_sys::Win32::{
            Foundation::HANDLE,
            Storage::FileSystem::{
                FILE_RENAME_INFO, FILE_RENAME_INFO_0, FileRenameInfo, FileRenameInfoEx,
                SetFileInformationByHandle,
            },
        };

        #[derive(Clone, Copy)]
        enum RenameKind {
            Legacy,
            Extended,
        }

        #[derive(Clone, Copy)]
        enum NameMode {
            Relative,
            Absolute,
        }

        let root = TestDirectory::new("rename-api-matrix");
        let cases = [
            (
                "legacy-create",
                RenameKind::Legacy,
                false,
                NameMode::Relative,
            ),
            (
                "legacy-replace",
                RenameKind::Legacy,
                true,
                NameMode::Relative,
            ),
            (
                "extended-create",
                RenameKind::Extended,
                false,
                NameMode::Relative,
            ),
            (
                "extended-replace",
                RenameKind::Extended,
                true,
                NameMode::Relative,
            ),
            (
                "extended-posix-replace",
                RenameKind::Extended,
                true,
                NameMode::Relative,
            ),
            (
                "extended-absolute-create",
                RenameKind::Extended,
                false,
                NameMode::Absolute,
            ),
            (
                "extended-absolute-replace",
                RenameKind::Extended,
                true,
                NameMode::Absolute,
            ),
            (
                "extended-absolute-posix-replace",
                RenameKind::Extended,
                true,
                NameMode::Absolute,
            ),
            (
                "legacy-absolute-create",
                RenameKind::Legacy,
                false,
                NameMode::Absolute,
            ),
            (
                "legacy-absolute-replace",
                RenameKind::Legacy,
                true,
                NameMode::Absolute,
            ),
        ];
        let mut successes = 0;

        for (label, kind, existing, name_mode) in cases {
            let parent = root.child(&format!("父目录-{label}"));
            let target_name = OsStr::new("published.json");
            let temporary_name = OsStr::new("temporary.json");
            let target_path = parent.join(target_name);
            if existing {
                fs::write(&target_path, b"old\n").expect("matrix target");
            }
            let directory = open_directory(&parent).expect("matrix directory");
            let mut temporary =
                create_regular_file_at(&directory, temporary_name).expect("matrix temporary");
            temporary.write_all(b"new\n").expect("matrix contents");
            temporary.sync_all().expect("matrix flush");
            let source_identity = identity(&temporary).expect("matrix source identity");
            let name = match name_mode {
                NameMode::Relative => target_name.encode_wide().collect::<Vec<_>>(),
                NameMode::Absolute => parent
                    .join(target_name)
                    .as_os_str()
                    .encode_wide()
                    .collect::<Vec<_>>(),
            };
            let name_bytes = name.len() * size_of::<u16>();
            let buffer_size = size_of::<FILE_RENAME_INFO>()
                .checked_add(name_bytes)
                .expect("matrix buffer size");
            let words = buffer_size.div_ceil(size_of::<usize>());
            let mut storage = vec![0usize; words];
            let information = storage.as_mut_ptr().cast::<FILE_RENAME_INFO>();
            let flags = match (kind, existing, label) {
                (RenameKind::Extended, true, "extended-posix-replace") => 0x0000_0003,
                (RenameKind::Extended, true, _) => 0x0000_0001,
                _ => 0,
            };
            let api_result = unsafe {
                std::ptr::write(
                    information,
                    FILE_RENAME_INFO {
                        Anonymous: match kind {
                            RenameKind::Legacy => FILE_RENAME_INFO_0 {
                                ReplaceIfExists: existing,
                            },
                            RenameKind::Extended => FILE_RENAME_INFO_0 { Flags: flags },
                        },
                        RootDirectory: match name_mode {
                            NameMode::Relative => directory.file.as_raw_handle() as HANDLE,
                            NameMode::Absolute => 0 as HANDLE,
                        },
                        FileNameLength: u32::try_from(name_bytes).expect("matrix name length"),
                        FileName: [0],
                    },
                );
                std::ptr::copy_nonoverlapping(
                    name.as_ptr(),
                    (*information).FileName.as_mut_ptr(),
                    name.len(),
                );
                SetFileInformationByHandle(
                    temporary.as_raw_handle() as HANDLE,
                    match kind {
                        RenameKind::Legacy => FileRenameInfo,
                        RenameKind::Extended => FileRenameInfoEx,
                    },
                    information.cast(),
                    u32::try_from(buffer_size).expect("matrix buffer length"),
                ) != 0
            };
            let api_error = (!api_result).then(io::Error::last_os_error);
            drop(temporary);
            let entries = fs::read_dir(&parent)
                .expect("matrix enumerate")
                .map(|entry| {
                    entry
                        .expect("matrix entry")
                        .file_name()
                        .to_string_lossy()
                        .into_owned()
                })
                .collect::<BTreeSet<_>>();
            let target_identity = open_regular_file_at(&directory, target_name)
                .ok()
                .and_then(|file| identity(&file).ok());
            let identity_matches = target_identity == Some(source_identity);
            let expected_success = matches!(name_mode, NameMode::Absolute);
            assert_eq!(
                api_result, expected_success,
                "rename matrix {label}: error={api_error:?} entries={entries:?} target_identity_matches_source={identity_matches}"
            );
            if expected_success {
                assert_eq!(
                    entries,
                    BTreeSet::from(["published.json".to_owned()]),
                    "rename matrix {label} published entries"
                );
                assert!(identity_matches, "rename matrix {label} identity");
                successes += 1;
            } else {
                assert_eq!(
                    api_error.as_ref().and_then(io::Error::raw_os_error),
                    Some(87),
                    "rename matrix {label} error"
                );
                let expected_entries = if existing {
                    BTreeSet::from(["published.json".to_owned(), "temporary.json".to_owned()])
                } else {
                    BTreeSet::from(["temporary.json".to_owned()])
                };
                assert_eq!(entries, expected_entries, "rename matrix {label} entries");
                assert!(!identity_matches, "rename matrix {label} identity");
            }
        }

        assert!(successes > 0, "rename API matrix found no successful cell");
    }

    #[test]
    fn open_or_create_directory_creates_and_binds_a_nested_chain() {
        let root = TestDirectory::new("nested-directory-chain");
        let target = root
            .path
            .join("a".repeat(120))
            .join("b".repeat(100))
            .join("owner")
            .join("ppt")
            .join("task");
        assert!(target.as_os_str().encode_wide().count() > 260);

        let directory = open_or_create_directory(&target)
            .unwrap_or_else(|error| panic!("nested directory creation failed: {error:?}"));

        assert_eq!(directory.path(), target);
        assert!(
            directory
                .same_identity()
                .expect("nested directory identity")
        );
        assert_eq!(
            open_directory(&target)
                .expect("reopen nested directory")
                .identity(),
            directory.identity()
        );

        let temporary_name = OsStr::new("artifact.tmp");
        let target_name = OsStr::new("artifact.pptx");
        let mut temporary = create_regular_file_at(&directory, temporary_name)
            .expect("create long-path temporary artifact");
        temporary
            .write_all(b"long-path-artifact")
            .expect("write long-path temporary artifact");
        temporary
            .sync_all()
            .expect("flush long-path temporary artifact");
        replace_file_at(
            &directory,
            temporary_name,
            target_name,
            &temporary,
            false,
            None,
        )
        .expect("publish long-path artifact");
        drop(temporary);
        let mut published = open_regular_file_at(&directory, target_name)
            .expect("reopen long-path published artifact");
        let mut body = Vec::new();
        published
            .read_to_end(&mut body)
            .expect("read long-path published artifact");
        assert_eq!(body, b"long-path-artifact");
    }

    #[test]
    fn windows_replace_file_at_binds_absolute_name_and_exact_buffer_size() {
        let root = TestDirectory::new("replace-file-at");

        assert_replace_case(
            &root.child("父目录-已有"),
            OsStr::new("已有-目标.json"),
            OsStr::new("临时-已有.json"),
            Some(b"old payload\n"),
        );
        assert_replace_case(
            &root.child("父目录-首次"),
            OsStr::new("首次-目标.json"),
            OsStr::new("临时-首次.json"),
            None,
        );

        // On the supported Windows targets, offset_of(FileName) is 20 and
        // the machine-word alignment is 8, so this two-WCHAR basename makes
        // the historical offset + filename_bytes calculation exactly aligned.
        assert_eq!(
            (std::mem::offset_of!(FILE_RENAME_INFO, FileName)
                + OsStr::new("ab").encode_wide().count() * std::mem::size_of::<u16>())
                % std::mem::size_of::<usize>(),
            0
        );
        assert_replace_case(
            &root.child("父目录-对齐"),
            OsStr::new("ab"),
            OsStr::new("tmp-ab"),
            Some(b"aligned old\n"),
        );
    }

    #[test]
    fn replace_file_at_rejects_a_rebound_existing_target() {
        let root = TestDirectory::new("replace-rebound-target");
        let parent = root.child("parent");
        let directory = open_or_create_directory(&parent).expect("open parent");
        let target_name = OsStr::new("published.json");
        let backup_name = OsStr::new("published.old.json");
        let temporary_name = OsStr::new("temporary.json");
        let target_path = parent.join(target_name);
        let backup_path = parent.join(backup_name);
        let temporary_path = parent.join(temporary_name);

        fs::write(&target_path, b"original").expect("original target");
        let original = open_regular_file_for_replace_at(&directory, target_name)
            .expect("open original target for replacement");
        let mut temporary =
            create_regular_file_at(&directory, temporary_name).expect("create temporary");
        temporary
            .write_all(b"replacement")
            .expect("write temporary");
        temporary.sync_all().expect("flush temporary");

        fs::rename(&target_path, &backup_path).expect("move original target");
        fs::write(&target_path, b"attacker").expect("install rebound target");

        let result = replace_file_at(
            &directory,
            temporary_name,
            target_name,
            &temporary,
            true,
            Some(&original),
        );
        assert!(result.is_err(), "rebound target was overwritten");
        assert_eq!(fs::read(&target_path).expect("rebound target"), b"attacker");
        assert_eq!(
            fs::read(&backup_path).expect("original backup"),
            b"original"
        );
        assert_eq!(
            fs::read(&temporary_path).expect("unpublished temporary"),
            b"replacement"
        );
    }

    #[test]
    fn remove_directory_at_deletes_only_the_bound_directory() {
        let root = TestDirectory::new("remove-directory-at");
        let parent_path = root.child("parent");
        let parent = open_directory(&parent_path).expect("open parent");
        let name = OsStr::new("owned");
        let target_path = parent_path.join(name);
        fs::create_dir(&target_path).expect("create target directory");
        let target = open_directory(&target_path).expect("open target");
        let target_identity = target.identity();
        drop(target);

        remove_directory_at(&parent, name, target_identity)
            .unwrap_or_else(|error| panic!("remove bound directory: {error:?}"));
        assert!(!target_path.exists(), "bound directory remains");
    }

    #[test]
    fn delete_capable_file_open_uses_share_delete_before_disposition() {
        let root = TestDirectory::new("delete-capable-file");
        let parent = root.child("parent");
        let directory = open_directory(&parent).expect("open parent");
        let name = OsStr::new("owned.bin");
        let path = parent.join(name);
        let mut created = create_regular_file_at(&directory, name).expect("create file");
        created.write_all(b"owned-marker").expect("write file");
        created.sync_all().expect("sync file");
        drop(created);

        let read_handle = open_regular_file_at(&directory, name).expect("read handle");
        assert!(
            open_regular_file_for_delete_at(&directory, name).is_err(),
            "a non-share-delete read handle must block delete open"
        );
        drop(read_handle);

        let mut delete_handle =
            open_regular_file_for_delete_at(&directory, name).expect("delete-capable handle");
        let mut marker = Vec::new();
        delete_handle
            .read_to_end(&mut marker)
            .expect("read marker through delete handle");
        assert_eq!(marker, b"owned-marker");
        remove_open_file_at(&directory, name, &delete_handle).expect("dispose file");
        drop(delete_handle);
        assert!(!path.exists(), "disposed file remains");
    }
}

#[cfg(windows)]
pub fn remove_file_at(directory: &DirectoryHandle, name: &OsStr) -> io::Result<()> {
    use windows_sys::Win32::Storage::FileSystem::{DELETE, FILE_READ_ATTRIBUTES, OPEN_EXISTING};

    let path = entry_path(directory, name)?;
    let file = open_with_flags(&path, DELETE | FILE_READ_ATTRIBUTES, OPEN_EXISTING, false)?;
    let file = validate_regular_file(file)?;
    dispose_file_by_handle(&file)
}

#[cfg(windows)]
pub fn remove_open_file_at(
    directory: &DirectoryHandle,
    name: &OsStr,
    file: &File,
) -> io::Result<()> {
    validate_entry_name(name)?;
    if !directory.same_identity()? {
        return Err(io::Error::other("directory parent changed"));
    }
    identity(file)?;
    if !directory.same_identity()? {
        return Err(io::Error::other("directory parent changed"));
    }
    dispose_file_by_handle(file)
}

#[cfg(windows)]
pub fn remove_open_file_at_bound(
    directory: &DirectoryHandle,
    name: &OsStr,
    file: &File,
) -> io::Result<()> {
    validate_entry_name(name)?;
    if identity(&directory.file)? != directory.identity {
        return Err(io::Error::other("directory handle changed"));
    }
    identity(file)?;
    dispose_file_by_handle(file)
}

#[cfg(windows)]
#[allow(unsafe_code)]
pub fn replace_file_at(
    directory: &DirectoryHandle,
    temporary_name: &OsStr,
    target_name: &OsStr,
    temporary_file: &File,
    replace_if_exists: bool,
    target_file: Option<&File>,
) -> io::Result<()> {
    use std::{mem::size_of, os::windows::io::AsRawHandle, ptr};
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_RENAME_INFO, FILE_RENAME_INFO_0, FileRenameInfoEx, SetFileInformationByHandle,
    };

    validate_entry_name(temporary_name)?;
    validate_entry_name(target_name)?;
    verify_replace_target_binding(directory, target_name, replace_if_exists, target_file)?;
    let target_name = windows_api_path_wide(&directory.path.join(target_name));
    if target_name.is_empty() || target_name.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "file name must not be empty or contain NUL",
        ));
    }
    let file_name_bytes = target_name
        .len()
        .checked_mul(size_of::<u16>())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "file name is too long"))?;
    let file_name_length = u32::try_from(file_name_bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "file name is too long"))?;
    let information_size = size_of::<FILE_RENAME_INFO>()
        .checked_add(file_name_bytes)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "rename information is too large",
            )
        })?;
    let information_size_u32 = u32::try_from(information_size).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "rename information is too large",
        )
    })?;
    let storage_words = information_size.div_ceil(size_of::<usize>());
    let mut storage = vec![0usize; storage_words];
    let information = storage.as_mut_ptr().cast::<FILE_RENAME_INFO>();
    const FILE_RENAME_FLAG_REPLACE_IF_EXISTS: u32 = 0x0000_0001;
    const FILE_RENAME_FLAG_POSIX_SEMANTICS: u32 = 0x0000_0002;
    let rename_flags = if replace_if_exists {
        FILE_RENAME_FLAG_REPLACE_IF_EXISTS | FILE_RENAME_FLAG_POSIX_SEMANTICS
    } else {
        0
    };
    // SAFETY: storage is aligned for FILE_RENAME_INFO, has at least
    // information_size bytes, and remains live for the complete call. The
    // trailing FileName array is sized by the explicit buffer length.
    let success = unsafe {
        ptr::write(
            information,
            FILE_RENAME_INFO {
                Anonymous: FILE_RENAME_INFO_0 {
                    Flags: rename_flags,
                },
                // The absolute destination is derived from the stable path
                // captured while opening the verified directory chain. The
                // source handle, final parent, and every ancestor remain held
                // without FILE_SHARE_DELETE, so the destination cannot escape
                // or rebind to a different directory tree.
                RootDirectory: 0 as windows_sys::Win32::Foundation::HANDLE,
                FileNameLength: file_name_length,
                FileName: [0],
            },
        );
        ptr::copy_nonoverlapping(
            target_name.as_ptr(),
            (*information).FileName.as_mut_ptr(),
            target_name.len(),
        );
        SetFileInformationByHandle(
            temporary_file.as_raw_handle() as windows_sys::Win32::Foundation::HANDLE,
            FileRenameInfoEx,
            information.cast(),
            information_size_u32,
        )
    } != 0;
    if !success {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
