import { fileURLToPath, pathToFileURL } from "node:url";
import path from "node:path";

import { resolve as resolveSource } from "./ts-source-loader.mjs";

const testRoot = path.dirname(fileURLToPath(import.meta.url));
const localforageMock = pathToFileURL(path.join(testRoot, "fixtures/localforage-mock.mjs")).href;
const authApiMock = pathToFileURL(path.join(testRoot, "fixtures/auth-api-mock.mjs")).href;

export async function resolve(specifier, context, nextResolve) {
  if (specifier === "localforage") {
    return { shortCircuit: true, url: localforageMock };
  }
  if (specifier === "@/lib/api") {
    return { shortCircuit: true, url: authApiMock };
  }
  return resolveSource(specifier, context, nextResolve);
}
