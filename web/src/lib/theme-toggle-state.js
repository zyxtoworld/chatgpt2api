import { writeOptionalStorageItem } from "./optional-storage.js";

const THEME_STORAGE_KEY = "chatgpt2api-theme";

export function toggleDocumentTheme(root, storage) {
  const isDark = !root.classList.contains("dark");
  root.classList.toggle("dark", isDark);
  root.style.colorScheme = isDark ? "dark" : "light";
  writeOptionalStorageItem(storage, THEME_STORAGE_KEY, isDark ? "dark" : "light");
  return isDark;
}
