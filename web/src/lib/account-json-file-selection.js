export async function settleAccountJsonFiles(files, parseFile) {
  const results = await Promise.all(
    files.map(async (file) => {
      try {
        const accounts = await parseFile(file);
        return { accounts, failed: accounts.length === 0 };
      } catch {
        return { accounts: [], failed: true };
      }
    }),
  );

  return {
    accounts: results.flatMap((result) => result.accounts),
    errorCount: results.filter((result) => result.failed).length,
  };
}
