import { resolve as resolveSource } from "./ts-source-loader.mjs";
import { fileURLToPath, pathToFileURL } from "node:url";

const localforageMock = pathToFileURL(fileURLToPath(new URL("./fixtures/localforage-mock.mjs", import.meta.url))).href;

export async function resolve(specifier, context, nextResolve) {
  if (specifier === "localforage") {
    return { shortCircuit: true, url: localforageMock };
  }
  return resolveSource(specifier, context, nextResolve);
}
