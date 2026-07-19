import { createHash } from "node:crypto";
import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { spawnSync } from "node:child_process";

const root = resolve(import.meta.dirname, "..");
const packageDirs = ["packages/ai", "packages/tui", "packages/agent", "packages/coding-agent"];

function run(command, args, options = {}) {
	const result = spawnSync(command, args, {
		cwd: options.cwd ?? root,
		encoding: "utf8",
		shell: process.platform === "win32" && command === "npm",
		stdio: options.capture ? "pipe" : "inherit",
	});
	if (result.status !== 0) {
		const details = [result.stdout, result.stderr].filter(Boolean).join("\n").trim();
		throw new Error(`${command} ${args.join(" ")} failed with exit code ${result.status}${details ? `:\n${details}` : ""}`);
	}
	return (result.stdout ?? "").trim();
}

function git(...args) {
	return run("git", args, { capture: true });
}

function sha256(path) {
	return createHash("sha256").update(readFileSync(path)).digest("hex");
}

function packageMetadata(packageDir) {
	return JSON.parse(readFileSync(join(root, packageDir, "package.json"), "utf8"));
}

function pack(packageDir, outputDir) {
	const stdout = run(
		"npm",
		["pack", resolve(root, packageDir), "--json", "--ignore-scripts", "--pack-destination", outputDir],
		{ capture: true },
	);
	const result = JSON.parse(stdout);
	if (!Array.isArray(result) || result.length !== 1 || !result[0].filename) {
		throw new Error(`Unexpected npm pack result for ${packageDir}: ${stdout}`);
	}
	return join(outputDir, result[0].filename);
}

const commit = git("rev-parse", "HEAD");
const branch = git("branch", "--show-current");
const status = git("status", "--porcelain");
if (status) {
	throw new Error("Refusing to create provenance from a dirty worktree. Commit or stash all changes first.");
}

let upstreamBase;
let upstreamTag;
try {
	upstreamBase = git("merge-base", "HEAD", "upstream/main");
	upstreamTag = git("describe", "--tags", "--exact-match", upstreamBase);
} catch {
	upstreamBase = git("rev-list", "--max-parents=0", "HEAD").split(/\r?\n/)[0];
	upstreamTag = "unknown";
}

const outputDir = resolve(root, ".artifacts", "lumicity", commit.slice(0, 12));
const packagesDir = join(outputDir, "packages");
const candidatePrefix = join(outputDir, "candidate-prefix");
rmSync(outputDir, { recursive: true, force: true });
mkdirSync(packagesDir, { recursive: true });

const artifacts = packageDirs.map((packageDir) => {
	const metadata = packageMetadata(packageDir);
	const path = pack(packageDir, packagesDir);
	return {
		name: metadata.name,
		version: metadata.version,
		path,
		sha256: sha256(path),
	};
});

run("npm", ["install", "--global", "--prefix", candidatePrefix, ...artifacts.map((item) => item.path)]);
const candidateRoot = run("npm", ["root", "--global", "--prefix", candidatePrefix], { capture: true });
const packageChecks = artifacts.map((artifact) => {
	const manifestPath = join(candidateRoot, ...artifact.name.split("/"), "package.json");
	const metadata = JSON.parse(readFileSync(manifestPath, "utf8"));
	if (metadata.name !== artifact.name || metadata.version !== artifact.version) {
		throw new Error(`Candidate identity mismatch for ${artifact.name}: ${metadata.name}@${metadata.version}`);
	}
	return { name: metadata.name, version: metadata.version, manifestPath };
});

const codingAgent = artifacts.find((artifact) => artifact.name === "@earendil-works/pi-coding-agent");
const cliPath = join(candidateRoot, "@earendil-works", "pi-coding-agent", "dist", "cli.js");
const cliVersion = run(process.execPath, [cliPath, "--version"], { capture: true });
if (!codingAgent || !cliVersion.includes(codingAgent.version)) {
	throw new Error(`Candidate CLI version mismatch: ${cliVersion}`);
}
const probeOutput = run(
	process.execPath,
	[join(root, "scripts", "probe-installed-abort.mjs"), "--root", candidateRoot],
	{ capture: true },
);
const abortProbe = JSON.parse(probeOutput);
if (!abortProbe.ok) throw new Error("Installed-runtime abort probe failed.");

const provenance = {
	schemaVersion: 1,
	createdAt: new Date().toISOString(),
	repository: "https://github.com/luminary19/Pi",
	branch,
	forkCommit: commit,
	upstreamBase,
	upstreamTag,
	packageIdentitiesPreserved: true,
	artifacts,
	candidate: {
		prefix: candidatePrefix,
		root: candidateRoot,
		cliPath,
		cliVersion,
		packages: packageChecks,
		abortProbe,
	},
};
const provenancePath = join(outputDir, "provenance.json");
writeFileSync(provenancePath, `${JSON.stringify(provenance, null, 2)}\n`);
console.log(JSON.stringify({ outputDir, provenancePath, artifacts, abortProbe }, null, 2));
