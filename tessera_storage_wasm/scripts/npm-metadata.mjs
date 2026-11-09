// wasm-pack names the generated package after the crate. The published
// browser package is @tessera-storage/sdk-wasm, so copy that identity
// from this directory's package.json onto pkg/package.json after a build.
import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const meta = JSON.parse(readFileSync(join(root, "package.json"), "utf8"));
const pkgPath = join(root, "pkg", "package.json");
const pkg = JSON.parse(readFileSync(pkgPath, "utf8"));
pkg.name = meta.name;
pkg.version = meta.version;
pkg.description = meta.description;
pkg.license = meta.license;
pkg.repository = meta.repository;
writeFileSync(pkgPath, JSON.stringify(pkg, null, 2) + "\n");
