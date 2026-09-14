import { existsSync } from "node:fs";
import { fileURLToPath, pathToFileURL } from "node:url";
import path from "node:path";

const sourceRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../src");

function candidates(filePath) {
  if (path.extname(filePath)) {
    return [filePath];
  }
  return [
    filePath,
    `${filePath}.ts`,
    `${filePath}.tsx`,
    `${filePath}.js`,
    path.join(filePath, "index.ts"),
    path.join(filePath, "index.js"),
  ];
}

function resolveSource(filePath) {
  return candidates(filePath).find((candidate) => existsSync(candidate));
}

export async function resolve(specifier, context, nextResolve) {
  let sourcePath;
  if (specifier.startsWith("@/")) {
    sourcePath = resolveSource(path.join(sourceRoot, specifier.slice(2)));
  } else if (specifier.startsWith(".") && context.parentURL?.startsWith("file:")) {
    sourcePath = resolveSource(fileURLToPath(new URL(specifier, context.parentURL)));
  }

  if (sourcePath) {
    return {
      shortCircuit: true,
      url: pathToFileURL(sourcePath).href,
    };
  }
  return nextResolve(specifier, context);
}
