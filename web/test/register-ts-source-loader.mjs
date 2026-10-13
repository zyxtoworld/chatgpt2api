import { register } from "node:module";

register("./ts-source-loader.mjs", { parentURL: import.meta.url });
