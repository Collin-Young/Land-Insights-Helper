import { spawn } from 'node:child_process';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

// Lightweight orchestrator that runs loginTrace for a random subset of counties.

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectRoot = path.resolve(__dirname, '..');

const rawArgs = process.argv.slice(2);
const normalizedOptions = parseBatchOptions(rawArgs);
const passThroughArgs = normalizedOptions.passThroughArgs;

async function main() {
  const counties = await loadCountyList(normalizedOptions.countyFile, normalizedOptions.state);
  const selectedCounties = normalizedOptions.runAll
    ? counties
    : pickRandomCounties(counties, normalizedOptions.count);

  console.log(
    `Running loginTrace for ${selectedCounties.length} county${selectedCounties.length === 1 ? '' : 'ies'} (${normalizedOptions.state})${
      normalizedOptions.runAll ? ' [full list]' : ''
    }.`
  );
  console.log(`Selected counties: ${selectedCounties.join(', ')}`);

  const results = [];
  for (const county of selectedCounties) {
    try {
      await runLoginTrace(county, passThroughArgs);
      results.push({ county, status: 'ok' });
    } catch (error) {
      results.push({ county, status: 'failed', error });
      if (!normalizedOptions.keepGoing) {
        break;
      }
    }
  }

  const failed = results.filter((result) => result.status === 'failed');
  if (failed.length) {
    failed.forEach((result) => {
      console.error(`Run failed for ${result.county}: ${result.error?.message ?? result.error}`);
    });
    process.exitCode = 1;
  } else {
    console.log('All county runs completed successfully.');
  }
}

function parseBatchOptions(args) {
  let count = parseInt(process.env.LANDINSIGHTS_BATCH_COUNT ?? '3', 10);
  let state = process.env.LANDINSIGHTS_STATE || 'Texas';
  let countyFile = process.env.LANDINSIGHTS_COUNTY_FILE || null;
  let keepGoing = false;
  let runAll = /^true$/i.test(process.env.LANDINSIGHTS_RUN_ALL ?? '');
  const recognized = new Set();

  for (const arg of args) {
    if (arg.startsWith('--count=')) {
      const value = Number(arg.split('=')[1]);
      if (Number.isFinite(value) && value > 0) {
        count = Math.floor(value);
      }
      recognized.add(arg);
    } else if (arg === '--all') {
      runAll = true;
      recognized.add(arg);
    } else if (arg.startsWith('--state=')) {
      const value = arg.split('=')[1];
      if (value) {
        state = value;
      }
      recognized.add(arg);
    } else if (arg.startsWith('--county-file=')) {
      const value = arg.split('=')[1];
      if (value) {
        countyFile = value;
      }
      recognized.add(arg);
    } else if (arg === '--keep-going') {
      keepGoing = true;
      recognized.add(arg);
    }
  }

  const passThroughArgs = args.filter((arg) => !recognized.has(arg));
  const stateSlug = state.trim().toLowerCase().replace(/\s+/g, '-');
  const resolvedCountyFile = countyFile
    ? path.resolve(projectRoot, countyFile)
    : path.resolve(projectRoot, 'data', `${stateSlug}-counties.json`);

  return {
    count,
    state: state.trim(),
    countyFile: resolvedCountyFile,
    runAll,
    keepGoing,
    passThroughArgs,
  };
}

async function loadCountyList(filePath, state) {
  try {
    const contents = await readFile(filePath, 'utf8');
    const parsed = JSON.parse(contents);
    const list = Array.isArray(parsed) ? parsed : [];
    const cleaned = list
      .map((entry) => (typeof entry === 'string' ? entry.trim() : ''))
      .filter(Boolean);
    if (!cleaned.length) {
      throw new Error('County file is empty.');
    }
    return cleaned;
  } catch (error) {
    throw new Error(`Unable to load county list for ${state}: ${error.message}`);
  }
}

function pickRandomCounties(counties, desiredCount) {
  if (!Array.isArray(counties) || !counties.length) {
    throw new Error('County list is missing.');
  }
  const count = Math.min(Math.max(1, desiredCount), counties.length);
  const shuffled = [...counties];
  for (let i = shuffled.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
  }
  return shuffled.slice(0, count);
}

function runLoginTrace(county, extraArgs) {
  const scriptPath = path.join(projectRoot, 'scripts', 'loginTrace.mjs');
  console.log(`\n>>> Starting Land Insights automation for ${county}`);

  return new Promise((resolve, reject) => {
    const child = spawn('node', [scriptPath, ...extraArgs], {
      cwd: projectRoot,
      env: { ...process.env, LANDINSIGHTS_COUNTY: county },
      stdio: 'inherit',
    });
    child.on('error', reject);
    child.on('exit', (code, signal) => {
      if (code === 0) {
        console.log(`Completed run for ${county}`);
        resolve();
      } else {
        reject(new Error(`loginTrace exited with code ${code ?? signal}`));
      }
    });
  });
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
