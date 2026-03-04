import { chromium } from 'playwright';
import dotenv from 'dotenv';
import path from 'node:path';
import { mkdir, access } from 'node:fs/promises';

dotenv.config();

const email = process.env.LANDINSIGHTS_EMAIL;
const password = process.env.LANDINSIGHTS_PASSWORD;

if (!email || !password) {
  console.error(
    'Missing credentials. Set LANDINSIGHTS_EMAIL and LANDINSIGHTS_PASSWORD in your environment or .env file.'
  );
  process.exit(1);
}

const headless = process.argv.includes('--headless');
const autoMode = process.argv.includes('--auto');
const holdArg = process.argv.find((arg) => arg.startsWith('--hold-ms='));
const holdMs = holdArg ? Number(holdArg.split('=')[1]) : 10000;
const slowArg = process.argv.find((arg) => arg.startsWith('--slow='));
const slowMo = slowArg ? Number(slowArg.split('=')[1]) : 50;
const countyName = process.env.LANDINSIGHTS_COUNTY || 'Travis County, TX';
const acresFrom = parseNumeric(process.env.LANDINSIGHTS_ACRES_FROM, 0);
const acresTo = parseNumeric(process.env.LANDINSIGHTS_ACRES_TO, 10000);
const maxParcelsPerExport = Math.max(1, parseNumeric(process.env.LANDINSIGHTS_MAX_EXPORT, 100000));
const acresIncrement = normalizeIncrement(parseNumeric(process.env.LANDINSIGHTS_ACRE_INCREMENT, 0.01));
const shouldAutomateExport = process.env.LANDINSIGHTS_ENABLE_EXPORT !== 'false';
const acreSearchTolerance = Math.max(acresIncrement / 2, 0.0001);
const acreOutputPrecision = 4;
const acresRefreshDelayMs = Math.max(0, parseNumeric(process.env.LANDINSIGHTS_ACRE_REFRESH_DELAY, 1800));
const preferredSmallAcreUpperBound = parseNumeric(process.env.LANDINSIGHTS_SMALL_ACRE_MAX, 0.99);
const minPreferredParcelCount = Math.max(1000, parseNumeric(process.env.LANDINSIGHTS_MIN_BATCH_COUNT, 75000));
const maxBatchIterations = Math.max(1, parseNumeric(process.env.LANDINSIGHTS_MAX_BATCHES, 40));
const parcelCountTimeoutMs = Math.max(5000, parseNumeric(process.env.LANDINSIGHTS_COUNT_TIMEOUT_MS, 60000));
const configuredTokenBudget = parseNumeric(process.env.LANDINSIGHTS_TOKEN_BALANCE, NaN);
const initialTokenBudget = Number.isFinite(configuredTokenBudget) ? configuredTokenBudget : null;
let remainingTokenBudget = initialTokenBudget;

const selectors = {
  email: 'xpath=//*[@id="root"]/div/div[1]/div/div/form/div[1]/span/span[2]/input',
  password: 'xpath=//*[@id="root"]/div/div[1]/div/div/form/div[1]/div/span/span[2]/input',
  submit: 'xpath=//*[@id="root"]/div/div[1]/div/div/form/button[1]',
};

const filterSelectors = {
  countyInput: [
    'xpath=//*[@id=":r2b:"]/div[1]/div[1]/div[2]/div[2]/div/div/div[1]/div/div/div[1]',
    'css=[data-testid="counties-search-autocomplete"] input',
    'css=input[placeholder*="county" i]',
    'text="Enter county name..."',
  ],
  countySelectionContainers: [
    'xpath=//*[@id=":r2b:"]/div[1]/div[1]/div[2]/div[2]',
    'css=[data-testid="counties-search-autocomplete"]',
  ],
  countyMenu: '[id^="downshift-"][id$="-menu"]',
  countyResult: '[id^="downshift-"][id$="-item-0"]',
  acresFrom: ['xpath=//*[@id="acresFrom"]', 'css=input#acresFrom'],
  acresTo: ['xpath=//*[@id="acresTo"]', 'css=input#acresTo'],
  aiDropdown: [
    'xpath=//*[@id=":r2b:"]/div[1]/div[1]/div[2]/div[3]/button',
    'xpath=//*[@id=":r2b:"]/div[1]/div[1]/div[2]/div[3]/button/span/span/span[2]/span',
    'xpath=//*[@id=":r2b:"]/div[1]/div[1]/div[2]/div[3]/button/span/span/span[2]/span/svg',
    'css=button:has-text("AI Scrubbing")',
    'text=/AI Scrubbing/i',
  ],
  advancedDropdown: [
    'xpath=//*[@id=":r2b:"]/div[1]/div[1]/div[2]/div[4]/button',
    'css=button:has-text("Advanced Scrubbing")',
    'text=/Advanced Scrubbing/i',
  ],
  miscDropdown: [
    'xpath=//*[@id=":r2b:"]/div[1]/div[1]/div[2]/div[11]/button/span/span/span[2]/span/svg',
    'xpath=//*[@id=":r2b:"]/div[1]/div[1]/div[2]/div[12]/button/span/span/span[2]/span/svg',
    'xpath=//*[@id=":r2b:"]/div[1]/div[1]/div[2]/div[11]/button',
    'xpath=//*[@id=":r2b:"]/div[1]/div[1]/div[2]/div[12]/button',
    'css=button:has-text("Misc Export Options")',
  ],
  exportButton: [
    'xpath=//*[@id=":r2b:"]/div[1]/div[1]/div[3]/div/div[2]/button',
    'xpath=//*[@id=":r2b:"]/div[1]/div[1]/div[3]/div[2]/div[2]/button',
    'css=button:has-text("Export")',
    'role=button[name=/Export/i]',
    'text=/^\s*Export/i',
  ],
  exportCountText: [
    'xpath=//*[@id=":r2b:"]/div[1]/div[1]/div[3]/div/div[2]/button/span',
    'xpath=//*[@id=":r2b:"]/div[1]/div[1]/div[3]/div/div[2]/button/span/span',
    'xpath=//*[@id=":r2b:"]/div[1]/div[1]/div[3]/div[2]/div[2]/button/span',
    'xpath=//*[@id=":r2b:"]/div[1]/div[1]/div[3]/div[2]/div[2]/button/span/span',
    'xpath=//*[@id=":r2b:"]/div/div[1]/div[3]/div/div[2]/button/span',
    'xpath=//*[@id=":r2b:"]/div/div[1]/div[3]/div/div[2]/button/span/span',
    'xpath=(//div[contains(@class,"export")]//button//span[contains(@class,"MuiButton")])[1]',
    'css=button:has-text("Export") span:has-text("Parcel")',
    'css=button:has-text("Export Parcels") span',
  ],
  exportCountLabels: [
    'xpath=//*[@id=":r2b:"]//div[contains(text(),"Parcels")]',
    'xpath=//*[@id=":r2b:"]//span[contains(text(),"Parcels")]',
    'css=[data-testid="selected-count"]',
    'text=/Parcels$/i',
  ],
  parcelKmlToggle: {
    stateSelectors: [
      '#includeParcelKml',
      '#includeKml',
      'xpath=//*[@id=":r2b:"]/div[1]/div[1]/div[2]/div[11]//input',
      'xpath=//*[@id=":r2b:"]/div[1]/div[1]/div[2]/div[12]//input',
    ],
    controlSelectors: [
      'role=switch[name="Include Parcel KML"]',
      'xpath=//*[@id=":r2b:"]/div[1]/div[1]/div[2]/div[11]//label[contains(.,"Include Parcel KML")]',
      'xpath=//*[@id=":r2b:"]/div[1]/div[1]/div[2]/div[12]//label[contains(.,"Include Parcel KML")]',
      'xpath=//*[@id=":r2b:"]/div[1]/div[1]/div[2]/div[11]/div/div/div/label/span',
      'xpath=//*[@id=":r2b:"]/div[1]/div[1]/div[2]/div[12]/div/div/div/label/span',
      '#includeParcelKml + span',
    ],
  },
  aiToggles: [
    {
      name: 'Keep Only Vacant Land (AI)',
      switchName: 'Keep Only Vacant Land',
      controlSelectors: [
        'xpath=//*[@id=":r2b:"]/div[1]/div[1]/div[2]/div[3]/div/div/div[1]/label',
        '#aiVacant + span',
        '[data-testid="aiVacant"]',
        '#aiVacant',
        'xpath=//*[@id=":r2b:"]/div[1]/div[1]/div[2]/div[3]/div/div/div[1]//button',
        'css=div:has-text("Keep Only Vacant Land") button',
      ],
      stateSelectors: ['#aiVacant', 'xpath=//*[@id=":r2b:"]/div[1]/div[1]/div[2]/div[3]/div/div/div[1]//input'],
      desiredState: false,
    },
    {
      name: 'Remove Bad Slope Land (AI)',
      switchName: 'Remove Bad Slope Land',
      controlSelectors: [
        'xpath=//*[@id=":r2b:"]/div[1]/div[1]/div[2]/div[3]/div/div/div[2]/label',
        '#aiBadSlope + span',
        '[data-testid="aiBadSlope"]',
      ],
      stateSelectors: ['#aiBadSlope', 'xpath=//*[@id=":r2b:"]/div[1]/div[1]/div[2]/div[3]/div/div/div[2]//input'],
      desiredState: false,
    },
    {
      name: 'Remove Land Locked Land (AI)',
      switchName: 'Remove Land Locked Land',
      controlSelectors: [
        'xpath=//*[@id=":r2b:"]/div[1]/div[1]/div[2]/div[3]/div/div/div[3]/label',
        '#aiLandlocked + span',
        '[data-testid="aiLandlocked"]',
      ],
      stateSelectors: ['#aiLandlocked', 'xpath=//*[@id=":r2b:"]/div[1]/div[1]/div[2]/div[3]/div/div/div[3]//input'],
      desiredState: false,
    },
    {
      name: 'Remove HOA Parcels (AI)',
      switchName: 'Remove HOA Parcels',
      controlSelectors: [
        'xpath=//*[@id=":r2b:"]/div[1]/div[1]/div[2]/div[3]/div/div/div[4]/label',
        '#aiHoa + span',
        '[data-testid="aiHoa"]',
      ],
      stateSelectors: ['#aiHoa', 'xpath=//*[@id=":r2b:"]/div[1]/div[1]/div[2]/div[3]/div/div/div[4]//input'],
      desiredState: false,
    },
  ],
};

const exportFlowSelectors = {
  reviewButton: [
    'xpath=//*[@id=":r2b:"]/div/div[1]/div[3]/div[3]/button/span',
    'xpath=//*[@id=":r2b:"]/div/div[1]/div[3]/div[3]/button',
    'xpath=//*[@id=":r2b:"]//button[span[contains(text(),"Purchase Now")]]',
    'css=button:has-text("Review")',
    'css=button:has-text("Purchase")',
    'css=button:has-text("Purchase Now")',
    'role=button[name=/Review|Purchase|Continue/i]',
    'role=button[name=/Purchase Now/i]',
    'text=/Review|Purchase|Continue/i',
  ],
  confirmPurchaseButton: [
    'xpath=//*[@id=":r8m:"]/div[2]/div/div[3]/button[2]',
    'xpath=//*[@id=":r8m:"]/div[2]/div/div[3]//button[span[contains(text(),"Purchase")]][last()]',
    'xpath=//*[@id=":r8m:"]/div[2]/div//button[span[contains(text(),"Purchase")]][last()]',
    'xpath=//*[@id=":r8u:"]/div[2]/div/div[3]/button[2]',
    'xpath=//*[@id=":r8u:"]/div[2]/div//button[span[contains(text(),"Purchase")]][last()]',
    'xpath=//*[@id=":r8u:"]/div[2]/div/div[3]//button[last()]',
    'xpath=//*[@id=":r8i:"]/div[2]/div/div[3]/button[2]',
    'xpath=//*[@id=":r8i:"]/div[2]/div/div[3]//button[span[contains(text(),"Purchase")]][last()]',
    'css=.lui-modal.lui-modal-open button.lui-button-primary:has-text("Purchase")',
    'css=.lui-modal.lui-modal-open button:has-text("Purchase")',
    'xpath=//div[contains(@class,"lui-modal") and contains(@class,"lui-modal-open")]//button[span[contains(text(),"Purchase")]][last()]',
    'css=[data-floating-ui-portal] button:has-text("Purchase")',
  ],
  confirmPurchaseDialog: [
    'xpath=//*[@id=":r8m:"]/div[2]/div',
    'xpath=//*[@id=":r8i:"]/div[2]',
    'xpath=//*[@id=":r8i:"]/div[2]/div',
    'xpath=//*[@id=":r8u:"]/div[2]/div',
    'xpath=//div[contains(@class,"lui-modal") and contains(@class,"lui-modal-open")]',
    'css=.lui-modal.lui-modal-open:has-text("Confirm Purchase")',
    'css=.lui-modal.lui-modal-open',
    'css=div[role="dialog"]:has-text("Confirm Purchase")',
    'css=[data-floating-ui-portal] .lui-modal',
    'css=[data-floating-ui-portal] div[role="dialog"]',
    'role=dialog[name=/Confirm Purchase/i]',
  ],
  fileNameContinueButton: [
    'xpath=//*[@id=":r8a:"]/div[2]/div/form/div[3]/button[2]/span',
    'xpath=//*[@id=":r8a:"]//button[span[contains(text(),"Continue")]][last()]',
    'xpath=(//*[@role="dialog"]//button[span[contains(text(),"Continue")]][last()])[last()]',
    'xpath=//*[@role="dialog"]//button[contains(.,"Continue")]',
    'css=form button:has-text("Continue")',
    'role=button[name=/Continue/i]',
  ],
  downloadButton: [
    'xpath=//*[@id=":r2b:"]/div/div[1]/div[3]/button[2]/span',
    'xpath=//*[@id=":r2b:"]/div/div[1]/div[3]/button[2]',
    'css=button:has-text("Download Clean Parcels")',
    'role=button[name=/Download Clean Parcels/i]',
    'text=/Download Clean Parcels/i',
  ],
  finalizePurchaseButton: [
    'xpath=//*[@id=":r2b:"]/div/div[1]/div[3]/div[3]/button',
    'xpath=//*[@id=":r2b:"]/div/div[1]/div[3]/div[3]/button/span',
    'xpath=//*[@id=":r2b:"]/div/div[1]/div[3]/div[3]//button[span[contains(text(),"Purchase")]]',
    'css=div[id=":r2b:"] div.lui-flex.lui-justify-end button:has-text("Purchase Now")',
    'css=button:has-text("Purchase Now")',
    'role=button[name=/Purchase Now/i]',
    'text=/Purchase Now/i',
  ],
  finalConfirmButton: [
    'xpath=//*[@id=":r8e:"]/div[2]/div/div[3]/button[2]',
    'xpath=//*[@id=":r8e:"]/div[2]/div/div[3]/button[2]/span',
    'xpath=//*[@role="dialog"]//button[span[contains(text(),"Purchase")]][last()]',
    'css=div[role="dialog"] button:has-text("Purchase")',
    'role=button[name=/Purchase/i]',
  ],
  downloadCloseButton: [
    'xpath=//*[@id=":r2b:"]/div/div[1]/div[1]/div[2]/button',
    'xpath=//*[@id=":r2b:"]/div/div[1]/div[1]/div[2]/button/span',
    'xpath=//*[@id=":r2b:"]/div/div[1]/div[1]/div[2]/button/span/svg',
    'xpath=//*[@id=":r2b:"]/div/div[1]/div[1]/div[2]/button/span/svg/path',
    'css=button[aria-label="Close"]',
    'role=button[name=/Close/i]',
  ],
};

const ignoredCountSelectors = [
  'xpath=//*[@id=":r2b:"]/div[1]/div[1]/div[1]/div/button[1]/span/span/span[2]',
  'xpath=//*[@id=":r2b:"]/div/div[1]/div[1]/div/button[1]/span/span/span[2]',
  'css=button:has-text("credits") span',
  'text=/credits$/i',
];

const acresFieldCache = {
  from: null,
  to: null,
};

function resetAcreFieldCache() {
  acresFieldCache.from = null;
  acresFieldCache.to = null;
}

const navigationSteps = [
  {
    name: 'dashboard menu',
    candidates: [
      'xpath=//*[@id="root"]/div/div/div[1]/div[1]/div[1]/div[1]/button',
      'xpath=//*[@id="root"]/div/div/div[1]/div[1]/div[1]/div[1]/button/span',
      'role=button[name="Open menu"]',
      'role=button[name="Dashboard menu"]',
      'text=/^\\s*Dashboard/',
    ],
    waitForSelector: 'text=Data Platform',
    waitForSelectorTimeout: 5000,
    waitAfter: 500,
  },
  {
    name: 'data platform dropdown',
    candidates: [
      'xpath=//*[@id=":r5:"]/div[1]/div[1]/div[2]/div[4]/div[2]',
      'xpath=//*[@id=":r5:"]/div[1]/div[1]/div[2]/div[4]//button',
      'text=Data Platform',
      'role=button[name=/Data Platform/i]',
    ],
    waitForSelector: 'text=/Map/i',
    waitForSelectorTimeout: 8000,
    waitAfter: 500,
    useLastMatch: true,
    force: true,
    timeout: 8000,
  },
  {
    name: 'map option',
    candidates: [
      'xpath=//*[@id=":r83:"]/div[1]/div[1]/div[2]/div[5]/div[1]/div/span/button',
      'xpath=//*[@id=":r5:"]/div[1]/div[1]/div[2]/div[5]//button',
      'xpath=(//span[contains(normalize-space(.),"Map")]/ancestor::button)[last()]',
      'css=button:has-text("Map")',
      'role=button[name=/Map/i]',
      'text=Map',
    ],
    waitForUrlPattern: /app\.landinsights\.co\/data/i,
    waitForUrlTimeout: 45000,
    waitAfter: 1500,
    useLastMatch: true,
    timeout: 8000,
    fallback: async (page) => {
      await page.goto('https://app.landinsights.co/data?menu=%22export%22#4/39.6/-93.55', {
        waitUntil: 'domcontentloaded',
      });
      await page.waitForURL(/app\.landinsights\.co\/data\??/i, { timeout: 45000 });
      return true;
    },
  },
  {
    name: 'close export modal',
    candidates: [
      'xpath=//*[@id=":r29:"]/div[1]/div[1]/div[1]/div[2]/button',
      'xpath=//*[@id=":r29:"]/div[1]/div[1]/div[1]/div[2]/button/span/svg/path/..',
      'xpath=//*[@id=":r29:"]/div[1]/div[1]/div[1]/div[2]',
      'role=button[name=/Close/i]',
      'css=button[aria-label="Close"]',
      'css=[data-testid="CloseIcon"]',
      'text=/^\\s*[✕xX]\\s*$/',
    ],
    preferredState: 'visible',
    timeout: 5000,
    clickDelay: 80,
    waitAfter: 800,
    optional: true,
    useLastMatch: true,
    force: true,
    waitForSelector: 'xpath=//*[@id=":r29:"]',
    waitForSelectorTimeout: 8000,
    fallback: async (page) => {
      const closed = await page.evaluate(() => {
        const buttons = Array.from(document.querySelectorAll('button'));
        const target = buttons.find((btn) => {
          const text = (btn.textContent || '').trim().toLowerCase();
          const label = (btn.getAttribute('aria-label') || '').toLowerCase();
          return (
            text === 'close' ||
            text === '×' ||
            text === 'x' ||
            label.includes('close') ||
            label.includes('dismiss')
          );
        });
        if (target) {
          target.click();
          return true;
        }
        return false;
      });
      if (closed) {
        await page.waitForTimeout(500);
      }
      return closed;
    },
  },
  {
    name: 'Filter & Export Parcels button',
    candidates: [
      'xpath=//*[@id="root"]/div/div/div[4]/div[1]/button[2]',
      'xpath=//*[@id="root"]/div/div/div[4]/div[1]/button[2]/span[2]',
      'xpath=//button[.//span[contains(text(),"Filter & Export Parcels")]]',
      'role=button[name="Filter & Export Parcels"]',
      'text=/Filter & Export Parcels/i',
    ],
    preferredState: 'visible',
    timeout: 8000,
    force: true,
    waitAfter: 0,
  },
];

const filterExportPanelStep = navigationSteps.find((step) => step.name === 'Filter & Export Parcels button');

const artifactsDir = path.resolve(process.cwd(), 'artifacts');
const downloadsDir = path.join(artifactsDir, 'downloads');

await mkdir(artifactsDir, { recursive: true });
await mkdir(downloadsDir, { recursive: true });

async function clickFirstMatch(page, step) {
  let lastError;
  for (const selector of step.candidates) {
    const locator = step.useLastMatch
      ? page.locator(selector).last()
      : page.locator(selector).first();
    try {
      await locator.waitFor({ state: step.preferredState ?? 'visible', timeout: step.timeout ?? 5000 });
      await locator.scrollIntoViewIfNeeded();
      await locator.click({ delay: step.clickDelay ?? 120, force: step.force ?? false });
      console.log(`Clicked ${step.name} via ${selector}`);
      if (step.waitForSelector) {
        await page.waitForSelector(step.waitForSelector, {
          state: step.waitForSelectorState ?? 'visible',
          timeout: step.waitForSelectorTimeout ?? 5000,
        });
      }
      if (step.waitForUrlPattern) {
        await page.waitForURL(step.waitForUrlPattern, {
          timeout: step.waitForUrlTimeout ?? 30000,
        });
      }
      if (step.waitAfter) {
        await page.waitForTimeout(step.waitAfter);
      }
      return page;
    } catch (error) {
      lastError = error;
      // try next candidate
    }
  }
  if (step.fallback) {
    try {
      const handled = await step.fallback(page);
      if (handled) {
        if (step.waitAfter) {
          await page.waitForTimeout(step.waitAfter);
        }
        console.log(`Fallback succeeded for ${step.name}`);
        return page;
      }
    } catch (error) {
      lastError = error;
    }
  }
  if (step.optional) {
    console.warn(`Optional step "${step.name}" skipped: ${lastError}`);
    return page;
  }
  throw new Error(`Unable to locate ${step.name} with provided selectors. Last error: ${lastError}`);
}

async function fillFilterForm(page) {
  console.log('Configuring Filter Parcels form...');
  await ensureCountySelection(page, { force: true });

  await setAcresRange(page, { from: acresFrom, to: acresTo });

  await configureFilterTogglesAndOptions(page);

  try {
    await handleExportWorkflow(page);
  } catch (error) {
    console.error(`Automated export workflow failed: ${error.message}`);
  }
}

async function configureFilterTogglesAndOptions(page) {
  const aiDropdown = await ensureAccordionOpen(
    page,
    'AI Scrubbing',
    filterSelectors.aiDropdown,
    filterSelectors.aiToggles[0].stateSelectors[0]
  );
  await ensureAiToggles(page, aiDropdown);

  await ensureAccordionOpen(page, 'Advanced Scrubbing', filterSelectors.advancedDropdown);

  const miscAccordion = await ensureAccordionOpen(
    page,
    'Misc Export Options',
    filterSelectors.miscDropdown,
    filterSelectors.parcelKmlToggle.stateSelectors
  );
  await page.waitForTimeout(300);
  await miscAccordion.scrollIntoViewIfNeeded();
  await toggleSwitch(page, filterSelectors.parcelKmlToggle, true);
}

async function ensureCountySelection(page, options = {}) {
  const force = options.force ?? false;
  if (!force) {
    const alreadySelected = await isCountySelected(page).catch(() => false);
    if (alreadySelected) {
      return;
    }
    console.log('County selection not detected. Reapplying county filter...');
  }
  await applyCountySelection(page);
}

async function isCountySelected(page) {
  const normalized = countyName?.trim().toLowerCase();
  if (!normalized) {
    return false;
  }
  const containers = filterSelectors.countySelectionContainers || [];
  for (const selector of containers) {
    try {
      const locator = page.locator(selector).first();
      await locator.waitFor({ state: 'visible', timeout: 1200 });
      const text = (await locator.innerText())?.toLowerCase();
      if (text?.includes(normalized)) {
        return true;
      }
    } catch {
      // continue to next selector
    }
  }
  return false;
}

async function applyCountySelection(page) {
  const countyBaseLocator = await getFirstAvailableLocator(page, filterSelectors.countyInput);
  await countyBaseLocator.scrollIntoViewIfNeeded();
  await countyBaseLocator.click({ force: true });
  const countyLocator = await ensureTextInput(countyBaseLocator);
  await countyLocator.fill('');
  await countyLocator.type(countyName, { delay: 50 });
  await selectCountySuggestion(page, countyName);
  await page.waitForTimeout(300);
  const confirmed = await isCountySelected(page).catch(() => false);
  console.log(confirmed ? `County set to ${countyName}` : `County typed as ${countyName}, awaiting confirmation...`);
}

async function ensureAcreFieldLocators(page) {
  const ensureLocator = async (key, selectorList) => {
    if (acresFieldCache[key]) {
      try {
        await acresFieldCache[key].waitFor({ state: 'attached', timeout: 200 });
        return acresFieldCache[key];
      } catch {
        acresFieldCache[key] = null;
      }
    }
    acresFieldCache[key] = await getFirstAvailableLocator(page, selectorList, {
      state: 'visible',
      timeout: 4000,
    });
    return acresFieldCache[key];
  };
  return {
    from: await ensureLocator('from', filterSelectors.acresFrom),
    to: await ensureLocator('to', filterSelectors.acresTo),
  };
}

async function setAcresRange(page, range, options = {}) {
  const normalized = normalizeAcreRange(range);
  const { from: fromLocator, to: toLocator } = await ensureAcreFieldLocators(page);
  await fromLocator.scrollIntoViewIfNeeded();
  await fromLocator.fill('');
  await fromLocator.type(formatAcreInput(normalized.from));
  await toLocator.fill('');
  await toLocator.type(formatAcreInput(normalized.to));
  console.log(`Acres range set to ${formatAcreInput(normalized.from)} - ${formatAcreInput(normalized.to)}`);
  if (options.waitForRefresh) {
    await waitForAcreRefresh(page, options.refreshDelay);
  }
  return normalized;
}

function formatAcreInput(value) {
  if (!Number.isFinite(value)) {
    return '';
  }
  return Number(value).toLocaleString('en-US', {
    useGrouping: false,
    maximumFractionDigits: acreOutputPrecision,
  });
}

function normalizeAcreRange(range) {
  if (!range) {
    throw new Error('Acreage range payload is required.');
  }
  const fromValue = Number(range.from);
  const toValue = Number(range.to);
  if (!Number.isFinite(fromValue) || !Number.isFinite(toValue)) {
    throw new Error(`Invalid acreage bounds: ${JSON.stringify(range)}`);
  }
  if (fromValue > toValue) {
    console.warn(`Swapping acreage bounds (start ${fromValue} was greater than end ${toValue}).`);
    return { from: toValue, to: fromValue };
  }
  return { from: fromValue, to: toValue };
}

async function handleExportWorkflow(page) {
  const normalizedRange = normalizeAcreRange({ from: acresFrom, to: acresTo });
  let initialCount = await detectParcelCount(page, { initialDelay: 500 }).catch((error) => {
    console.warn(`Unable to determine parcel count: ${error.message}`);
    return null;
  });

  const exportDisabled = await isExportButtonDisabled(page).catch(() => false);

  if (initialCount != null) {
    console.log(`Export panel currently reports ${initialCount.toLocaleString()} parcels.`);
  } else if (!exportDisabled) {
    console.warn('Unable to determine parcel count and export button appears enabled. Skipping automated export.');
    return;
  } else {
    console.warn('Parcel count unavailable but export button is disabled. Assuming export limit exceeded.');
  }

  if (!shouldAutomateExport) {
    console.log('LANDINSIGHTS_ENABLE_EXPORT is set to false. Leaving export action for manual review.');
    return;
  }

  if (!exportDisabled && initialCount != null && initialCount < maxParcelsPerExport) {
    console.log(
      `Parcel count (${initialCount.toLocaleString()}) is within the ${maxParcelsPerExport.toLocaleString()} limit. Triggering single export.`
    );
    await processExportBatch(page, {
      batch: 1,
      from: normalizedRange.from,
      to: normalizedRange.to,
      count: initialCount,
      reopenAfter: false,
    });
    return;
  }

  const detectedLabel =
    initialCount != null ? initialCount.toLocaleString() : `an estimated ${maxParcelsPerExport.toLocaleString()}+`;
  console.log(
    `Detected ${detectedLabel} parcels. Splitting ${formatAcreInput(normalizedRange.from)} - ${formatAcreInput(
      normalizedRange.to
    )} into multiple exports capped at ${maxParcelsPerExport.toLocaleString()} parcels each.`
  );

  let currentStart = normalizedRange.from;
  let batchIndex = 1;
  let iterations = 0;

  const seeded = await trySeedSmallAcreBatch(page, normalizedRange).catch((error) => {
    console.warn(`Unable to seed with small-acre batch: ${error.message}`);
    return null;
  });
  if (seeded) {
    const seedLabel = seeded.count != null ? seeded.count.toLocaleString() : 'unknown';
    console.log(
      `Seed batch: acres ${formatAcreInput(normalizedRange.from)} - ${formatAcreInput(seeded.upperBound)} (${seedLabel} parcels).`
    );
    const nextStart = nextAcreStart(seeded.upperBound, normalizedRange.to);
    const hasMore = nextStart < normalizedRange.to;
    await processExportBatch(page, {
      batch: batchIndex,
      from: normalizedRange.from,
      to: seeded.upperBound,
      count: seeded.count,
      reopenAfter: hasMore,
    }).catch((error) => {
      throw new Error(`Seed batch export failed: ${error.message}`);
    });
    batchIndex += 1;
    if (!hasMore) {
      console.log('Seed batch covered the full requested acreage range.');
      return;
    }
    currentStart = nextStart;
  } else {
    await setAcresRange(page, { from: normalizedRange.from, to: normalizedRange.to }, { waitForRefresh: true });
  }

  while (currentStart < normalizedRange.to && iterations < maxBatchIterations) {
    iterations += 1;
    const batch = await findBatchUpperBound(page, currentStart, normalizedRange.to, maxParcelsPerExport);
    if (!batch) {
      console.warn('Unable to determine a follow-up acreage range under the export ceiling. Stopping automation.');
      break;
    }
    const { upperBound, count } = batch;
    const countLabel = count != null ? count.toLocaleString() : 'unknown';
    console.log(
      `Batch ${batchIndex}: acres ${formatAcreInput(currentStart)} - ${formatAcreInput(upperBound)} (${countLabel} parcels).`
    );
    const nextStart = nextAcreStart(upperBound, normalizedRange.to);
    const hasMore = nextStart < normalizedRange.to;
    try {
      await processExportBatch(page, {
        batch: batchIndex,
        from: currentStart,
        to: upperBound,
        count,
        reopenAfter: hasMore,
      });
    } catch (error) {
      console.error(`Failed to finish export for batch ${batchIndex}: ${error.message}`);
      break;
    }
    if (!hasMore) {
      break;
    }
    if (nextStart <= currentStart) {
      console.warn('Acreage boundary did not advance after batch export. Halting to avoid duplicate downloads.');
      break;
    }
    currentStart = nextStart;
    batchIndex += 1;
  }

  if (iterations >= maxBatchIterations) {
    console.warn('Reached maximum configured number of batches. Remaining parcels may require manual exports.');
  }
}

async function processExportBatch(page, meta = {}) {
  preflightTokenBudget(meta.count);
  await triggerExport(page, meta);
  await completeExportFlow(page, meta);
  applyTokenBudget(meta.count);
}

async function detectParcelCount(page, options = {}) {
  let timeoutId;
  try {
    return await Promise.race([
      detectParcelCountCore(page, options),
      new Promise((_, reject) => {
        timeoutId = setTimeout(() => {
          reject(new Error(`Parcel count detection timed out after ${parcelCountTimeoutMs}ms.`));
        }, parcelCountTimeoutMs);
      }),
    ]);
  } finally {
    if (timeoutId) {
      clearTimeout(timeoutId);
    }
  }
}

async function detectParcelCountCore(page, options = {}) {
  const numericPrevious = Number(options.previousCount);
  const normalizedOptions = {
    ...options,
    ensureDifferent: Boolean(options.ensureDifferent),
    previousCount: Number.isFinite(numericPrevious) ? numericPrevious : null,
  };

  const enforceDifference = (value) => {
    if (
      value != null &&
      normalizedOptions.ensureDifferent &&
      normalizedOptions.previousCount != null &&
      value === normalizedOptions.previousCount
    ) {
      throw new Error('Parcel count did not change from previous value.');
    }
    return value;
  };

  const attempts = [];
  try {
    return enforceDifference(await waitForExportCount(page, normalizedOptions));
  } catch (error) {
    attempts.push(error);
  }
  try {
    return enforceDifference(await readExportCountSnapshot(page));
  } catch (error) {
    attempts.push(error);
  }
  const fromLabels = await readCountFromSelectors(page, filterSelectors.exportCountLabels);
  const normalizedLabels = enforceDifference(fromLabels);
  if (normalizedLabels != null) {
    return normalizedLabels;
  }
  const fromDocument = await readCountFromDocument(page);
  const normalizedDocument = enforceDifference(fromDocument);
  if (normalizedDocument != null) {
    return normalizedDocument;
  }
  const lastError = attempts.pop();
  throw lastError || new Error('Unable to read parcel count.');
}

async function waitForExportCount(page, options = {}) {
  const timeout = options.timeout ?? 20000;
  const pollInterval = options.pollInterval ?? 600;
  const stableReadings = options.stableReadings ?? 2;
  const initialDelay = options.initialDelay ?? 0;
  const returnLastOnTimeout = options.returnLastOnTimeout ?? true;
  const previousCount = options.previousCount ?? null;
  const ensureDifferent = options.ensureDifferent ?? false;
  if (initialDelay > 0) {
    await page.waitForTimeout(initialDelay);
  }
  let locator = await getExportCountLocator(page);
  const deadline = Date.now() + timeout;
  let lastValue = null;
  let stableCount = 0;

  while (Date.now() < deadline) {
    try {
      const parsed = await readExportCountValue(locator);
      if (parsed != null) {
        if (parsed === lastValue) {
          stableCount += 1;
        } else {
          lastValue = parsed;
          stableCount = 1;
        }
        if (stableCount >= stableReadings) {
          if (ensureDifferent && previousCount != null && parsed === previousCount) {
            // continue waiting for a different value
          } else {
            return parsed;
          }
        }
      } else {
        stableCount = 0;
      }
    } catch (error) {
      locator = await getExportCountLocator(page);
      lastValue = null;
      stableCount = 0;
    }
    await page.waitForTimeout(pollInterval);
  }
  if (lastValue != null && returnLastOnTimeout) {
    if (ensureDifferent && previousCount != null && lastValue === previousCount) {
      throw new Error('Export count did not change from previous value before timeout.');
    }
    console.warn(
      `Export count did not stabilize within ${timeout}ms. Returning last observed value (${lastValue.toLocaleString()}).`
    );
    return lastValue;
  }
  throw new Error('Export count did not stabilize within the allotted time.');
}

async function getExportCountLocator(page) {
  const selectors = filterSelectors.exportCountText.filter((selector) => !shouldIgnoreCountSelector(selector));
  return getFirstAvailableLocator(page, selectors, {
    state: 'visible',
    timeout: 7000,
  });
}

async function readExportCountSnapshot(page) {
  const locator = await getExportCountLocator(page);
  const count = await readExportCountValue(locator);
  if (count == null) {
    throw new Error('Export count snapshot did not include numeric data.');
  }
  return count;
}

async function readExportCountValue(locator) {
  const texts = [];
  try {
    texts.push(await locator.innerText());
  } catch {
    // ignore
  }
  try {
    const extra = await locator.evaluate((node) => {
      const parts = [];
      const collect = (subject) => {
        if (!subject) return;
        const textContent = subject.textContent || subject.innerText;
        if (textContent) parts.push(textContent);
        const aria = subject.getAttribute?.('aria-label');
        if (aria) parts.push(aria);
      };
      collect(node);
      const button = node.closest?.('button');
      collect(button);
      return parts.filter(Boolean).join(' | ');
    });
    texts.push(extra);
  } catch {
    // ignore
  }
  for (const text of texts) {
    const parsed = parseParcelCount(text);
    if (parsed != null) {
      return parsed;
    }
  }
  return null;
}

async function readCountFromSelectors(page, selectors = []) {
  if (!selectors?.length) {
    return null;
  }
  for (const selector of selectors) {
    if (shouldIgnoreCountSelector(selector)) {
      continue;
    }
    try {
      const locator = page.locator(selector).first();
      await locator.waitFor({ state: 'visible', timeout: 1500 });
      const text = (await locator.innerText())?.trim();
      const parsed = parseParcelCount(text);
      if (parsed != null) {
        return parsed;
      }
    } catch {
      // continue
    }
  }
  return null;
}

async function readCountFromDocument(page) {
  try {
    const raw = await page.evaluate(() => {
      const pattern = /([0-9][0-9,\.]*)\s+Parcels/i;
      const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
      while (walker.nextNode()) {
        const text = walker.currentNode?.textContent || '';
        const match = text.match(pattern);
        if (match) {
          return match[1];
        }
      }
      return null;
    });
    return parseParcelCount(raw);
  } catch {
    return null;
  }
}

function parseParcelCount(text) {
  if (!text) {
    return null;
  }
  const lowered = text.toLowerCase();
  const mentionsParcels = lowered.includes('parcel');
  if (!mentionsParcels) {
    if (lowered.includes('credit') || lowered.includes('token') || lowered.includes('balance')) {
      return null;
    }
    return null;
  }
  const match = text.match(/([0-9][0-9,\.]*)/);
  if (!match) {
    return null;
  }
  const numeric = match[1].replace(/,/g, '');
  const parsed = Number(numeric);
  return Number.isFinite(parsed) ? parsed : null;
}

function shouldIgnoreCountSelector(selector) {
  return ignoredCountSelectors.some((ignored) => ignored === selector);
}

async function waitForAcreRefresh(page, delayOverride) {
  const delay = delayOverride ?? acresRefreshDelayMs;
  if (delay > 0) {
    await page.waitForTimeout(delay);
  }
}

async function triggerExport(page, meta = {}) {
  const button = await getFirstAvailableLocator(page, filterSelectors.exportButton, {
    state: 'visible',
    timeout: 8000,
  });
  await button.scrollIntoViewIfNeeded();
  if (await isLocatorDisabled(button)) {
    throw new Error('Export button is disabled.');
  }
  await button.click({ delay: 80, force: true });
  const countLabel = meta.count != null ? meta.count.toLocaleString() : 'unknown';
  console.log(
    `Requested export for acres ${formatAcreInput(meta.from)} - ${formatAcreInput(meta.to)} (${countLabel} parcels).`
  );
  await page.waitForTimeout(meta.postClickDelay ?? 1200);
}

async function findBatchUpperBound(page, lowerBound, maxBound, limit = maxParcelsPerExport) {
  const start = Number(lowerBound);
  const end = Number(maxBound);
  if (!Number.isFinite(start) || !Number.isFinite(end)) {
    return null;
  }
  if (start >= end) {
    await setAcresRange(page, { from: start, to: end }, { waitForRefresh: true });
    const singleCount = await detectParcelCount(page, { initialDelay: 200 }).catch(() => null);
    if (singleCount == null) {
      return null;
    }
    return { upperBound: end, count: singleCount };
  }

  let low = start;
  let high = end;
  let best = null;
  let iteration = 0;
  const maxIterations = 25;
  let previousCount = null;

  while (iteration < maxIterations) {
    iteration += 1;
    const candidate = iteration === 1 ? high : low + (high - low) / 2;
    await setAcresRange(page, { from: start, to: candidate }, { waitForRefresh: true });
    const count = await detectParcelCount(page, {
      initialDelay: 250,
      ensureDifferent: previousCount != null,
      previousCount,
    }).catch(() => null);
    if (count == null) {
      break;
    }
    if (count <= limit) {
      best = { upperBound: candidate, count };
      previousCount = count;
      const delta = Math.abs(limit - count);
      if (candidate >= end || delta <= Math.max(1000, limit * 0.01)) {
        break;
      }
      low = candidate;
    } else {
      const nextHigh = candidate - acreSearchTolerance;
      if (nextHigh <= start) {
        break;
      }
      high = nextHigh;
      previousCount = count;
    }
    if (high - low <= acreSearchTolerance) {
      break;
    }
  }

  if (!best) {
    return null;
  }

  await setAcresRange(page, { from: start, to: best.upperBound }, { waitForRefresh: true });
  const confirmedCount = await detectParcelCount(page, { initialDelay: 200 }).catch(() => best.count);
  return { upperBound: best.upperBound, count: confirmedCount ?? best.count };
}

function nextAcreStart(current, maxBound) {
  const bump = Math.max(acresIncrement, 1 / 10 ** (acreOutputPrecision + 1));
  return Math.min(maxBound, current + bump);
}

async function completeExportFlow(page, meta = {}) {
  console.log(
    `Completing export batch ${meta.batch ?? '?'}...` +
      (meta.count != null ? ` (${meta.count.toLocaleString()} parcels)` : '')
  );
  await waitAndClickSelectors(page, exportFlowSelectors.reviewButton, {
    timeout: 15000,
    description: 'Review/Purchase button',
  });
  let confirmClicked = false;
  let confirmUsedFallback = false;
  try {
    await clickConfirmPurchaseButton(page, { timeout: 8000 });
    confirmClicked = true;
  } catch (error) {
    console.warn(`Scoped confirm purchase click failed (${error.message}). Retrying with generic selectors...`);
  }
  if (!confirmClicked) {
    confirmUsedFallback = true;
    await waitForAnySelector(page, exportFlowSelectors.confirmPurchaseDialog, {
      state: 'visible',
      timeout: 8000,
    }).catch(() => {});
    await waitAndClickSelectors(page, exportFlowSelectors.confirmPurchaseButton, {
      timeout: 12000,
      selectorTimeout: 2500,
      description: 'Confirm purchase button',
    });
    await waitForAnySelector(page, exportFlowSelectors.confirmPurchaseDialog, {
      state: 'hidden',
      timeout: 5000,
    }).catch(() => {});
  }
  if (confirmUsedFallback) {
    const finalizeButton = await findLocatorOrNull(page, exportFlowSelectors.finalizePurchaseButton, {
      state: 'visible',
      timeout: 2500,
    });
    if (finalizeButton) {
      console.log('Legacy "Finalize Purchase Now" button detected after fallback; completing legacy flow.');
      await waitForButtonEnabled(page, finalizeButton, { timeout: 10000 }).catch(() => {});
      await finalizeButton.scrollIntoViewIfNeeded();
      await finalizeButton.click({ delay: 90, force: true });
      await waitAndClickSelectors(page, exportFlowSelectors.finalConfirmButton, {
        timeout: 12000,
        selectorTimeout: 2500,
        description: 'Final confirm purchase button',
      });
    } else {
      console.log('Finalize Purchase button not visible after fallback; assuming new export flow.');
    }
  } else {
    console.log('Skipping legacy final purchase flow (confirm dialog handled by new UI).');
  }
  try {
    await waitAndClickSelectors(page, exportFlowSelectors.fileNameContinueButton, {
      timeout: 12000,
      waitForEnabled: true,
      description: 'Filename continue button',
    });
  } catch (error) {
    console.warn(`Filename dialog not detected; continuing with default name. (${error.message})`);
  }
  console.log('Waiting for Download Clean Parcels button to become available...');
  const downloadLocator = await getFirstAvailableLocator(page, exportFlowSelectors.downloadButton, {
    state: 'visible',
    timeout: 300000,
  });
  await waitForButtonEnabled(page, downloadLocator, { page, timeout: 300000 });
  await downloadLocator.scrollIntoViewIfNeeded();
  const downloadPromise = page.waitForEvent('download', { timeout: meta.downloadTimeout ?? 300000 });
  try {
    await downloadLocator.click({ delay: 90, force: true });
  } catch (error) {
    downloadPromise.catch(() => {});
    throw error;
  }
  console.log('Download Clean Parcels triggered. Waiting for file...');
  try {
    const download = await downloadPromise;
    await saveExportDownload(download, meta);
  } catch (error) {
    console.error(`Download handling failed: ${error.message}`);
  }
  await page.waitForTimeout(meta.postDownloadDelay ?? 4000);
  await closeDownloadDrawer(page).catch(() => {});
  if (meta.reopenAfter) {
    await reopenFilterExportPanel(page);
  }
}

async function waitAndClickSelectors(page, selectors, options = {}) {
  const selectorTimeout = options.selectorTimeout ?? options.timeout ?? 10000;
  const locator = await getFirstAvailableLocator(page, selectors, {
    state: options.state ?? 'visible',
    timeout: selectorTimeout,
    useLastMatch: options.useLastMatch,
  });
  if (options.waitForEnabled) {
    await waitForButtonEnabled(page, locator, {
      timeout: options.enabledTimeout ?? options.timeout ?? 12000,
    }).catch(() => {});
  }
  await locator.scrollIntoViewIfNeeded();
  await locator.click({ delay: options.delay ?? 80, force: options.force ?? true });
  if (options.description) {
    console.log(`Clicked ${options.description}.`);
  }
  return locator;
}

async function waitForButtonEnabled(page, locator, options = {}) {
  const timeout = options.timeout ?? 180000;
  const pollInterval = options.pollInterval ?? 1000;
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    try {
      const disabled = await isLocatorDisabled(locator);
      if (!disabled) {
        return locator;
      }
    } catch {
      // ignore and retry
    }
    await page.waitForTimeout(pollInterval);
  }
  throw new Error('Button did not become enabled before timeout.');
}

async function closeDownloadDrawer(page) {
  try {
    const closeButton = await getFirstAvailableLocator(page, exportFlowSelectors.downloadCloseButton, {
      state: 'visible',
      timeout: 8000,
    });
    await closeButton.scrollIntoViewIfNeeded();
    await closeButton.click({ delay: 60, force: true });
    console.log('Closed download panel.');
    await page.waitForTimeout(300);
    return true;
  } catch {
    console.log('Download panel close button not detected; continuing.');
    return false;
  }
}

async function saveExportDownload(download, meta = {}) {
  const suggested = download?.suggestedFilename?.() || 'landinsights-export.csv';
  const extension = path.extname(suggested) || '.csv';
  const baseName = buildDownloadFileName(meta);
  const targetPath = await ensureUniqueDownloadPath(path.join(downloadsDir, `${baseName}${extension}`));
  await download.saveAs(targetPath);
  console.log(`Saved export to ${targetPath}`);
  return targetPath;
}

function buildDownloadFileName(meta = {}) {
  const { county, state } = splitCountyAndState(countyName);
  const countySegment = sanitizeForFilename(county, 'county');
  const stateSegment = sanitizeForFilename(state, 'state');
  const batchSegment = sanitizeForFilename(`batch-${meta.batch ?? 1}`, 'batch');
  const fromValue = Number.isFinite(meta.from) ? meta.from : acresFrom;
  const toValue = Number.isFinite(meta.to) ? meta.to : acresTo;
  const rangeSegment = sanitizeForFilename(
    `${formatAcreInput(fromValue)}-${formatAcreInput(toValue)}`,
    'acres'
  );
  const countSegment =
    meta.count != null && Number.isFinite(meta.count)
      ? sanitizeForFilename(meta.count.toString(), 'count')
      : 'count-unknown';
  return `${countySegment}-${stateSegment}-${batchSegment}-${rangeSegment}-${countSegment}`;
}

function splitCountyAndState(fullName) {
  if (!fullName) {
    return { county: 'county', state: 'state' };
  }
  const [countyPart, statePart] = fullName.split(',').map((part) => part.trim());
  return {
    county: countyPart || 'county',
    state: statePart || 'state',
  };
}

function sanitizeForFilename(value, fallback = 'value') {
  if (value == null) {
    return fallback;
  }
  const cleaned = value
    .toString()
    .normalize('NFKD')
    .replace(/[^a-zA-Z0-9]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');
  return cleaned || fallback;
}

async function ensureUniqueDownloadPath(initialPath) {
  let candidate = initialPath;
  let suffix = 1;
  const ext = path.extname(initialPath);
  const base = ext ? initialPath.slice(0, -ext.length) : initialPath;
  while (true) {
    try {
      await access(candidate);
      candidate = `${base}-${suffix}${ext}`;
      suffix += 1;
    } catch {
      return candidate;
    }
  }
}

function preflightTokenBudget(count) {
  if (remainingTokenBudget == null) {
    return;
  }
  if (!Number.isFinite(count)) {
    console.warn('Token budget configured but parcel count unavailable; proceeding without pre-check.');
    return;
  }
  if (count > remainingTokenBudget) {
    throw new Error(
      `Export requires ${count.toLocaleString()} tokens but only ${remainingTokenBudget.toLocaleString()} remain.`
    );
  }
}

function applyTokenBudget(count) {
  if (remainingTokenBudget == null || !Number.isFinite(count)) {
    return;
  }
  remainingTokenBudget = Math.max(0, remainingTokenBudget - count);
  console.log(
    `Token budget updated: ${remainingTokenBudget.toLocaleString()} token${
      remainingTokenBudget === 1 ? '' : 's'
    } remaining.`
  );
}

async function reopenFilterExportPanel(page) {
  if (!filterExportPanelStep) {
    throw new Error('Filter & Export Parcels button configuration missing.');
  }
  console.log('Re-opening Filter & Export Parcels panel for next acreage range...');
  resetAcreFieldCache();
  await clickFirstMatch(page, { ...filterExportPanelStep });
  await page.waitForTimeout(500);
  try {
    await waitForAnySelector(page, filterSelectors.countyInput, { state: 'visible', timeout: 15000 });
  } catch {
    await waitForAnySelector(page, filterSelectors.acresFrom, { state: 'visible', timeout: 15000 });
  }
  await page.waitForTimeout(300);
  await ensureCountySelection(page).catch(() => {});
  await configureFilterTogglesAndOptions(page);
}

async function isExportButtonDisabled(page) {
  try {
    const button = await getFirstAvailableLocator(page, filterSelectors.exportButton, {
      state: 'visible',
      timeout: 5000,
    });
    return isLocatorDisabled(button);
  } catch {
    return false;
  }
}

async function isLocatorDisabled(locator) {
  if (typeof locator.isDisabled === 'function') {
    try {
      return await locator.isDisabled();
    } catch {
      // fall through
    }
  }
  try {
    return await locator.evaluate((node) => {
      const ariaDisabled = node.getAttribute?.('aria-disabled');
      if (ariaDisabled != null) {
        return ariaDisabled === 'true';
      }
      if (typeof node.hasAttribute === 'function' && node.hasAttribute('disabled')) {
        return true;
      }
      return node.disabled === true;
    });
  } catch {
    return false;
  }
}

async function trySeedSmallAcreBatch(page, range) {
  const start = Number(range.from);
  const end = Number(range.to);
  if (!Number.isFinite(start) || !Number.isFinite(end)) {
    return null;
  }
  if (start > 0 || preferredSmallAcreUpperBound == null || !Number.isFinite(preferredSmallAcreUpperBound)) {
    return null;
  }
  if (preferredSmallAcreUpperBound <= start || preferredSmallAcreUpperBound >= end) {
    return null;
  }
  const seedUpperBound = preferredSmallAcreUpperBound;
  const originalRange = { from: start, to: end };
  await setAcresRange(page, { from: start, to: seedUpperBound }, { waitForRefresh: true });
  const count = await detectParcelCount(page, {
    initialDelay: 300,
    ensureDifferent: true,
    previousCount: maxParcelsPerExport,
  }).catch(() => null);
  if (count == null) {
    await setAcresRange(page, originalRange, { waitForRefresh: true });
    return null;
  }
  if (count > maxParcelsPerExport) {
    console.log(
      `Seed batch (${formatAcreInput(start)}-${formatAcreInput(seedUpperBound)}) is still ${count.toLocaleString()} parcels (> ${maxParcelsPerExport.toLocaleString()}). Skipping seed.`
    );
    await setAcresRange(page, originalRange, { waitForRefresh: true });
    return null;
  }
  if (count < minPreferredParcelCount) {
    console.log(
      `Seed batch (${formatAcreInput(start)}-${formatAcreInput(seedUpperBound)}) only has ${count.toLocaleString()} parcels (< ${minPreferredParcelCount.toLocaleString()}). Preferring adaptive split instead.`
    );
    await setAcresRange(page, originalRange, { waitForRefresh: true });
    return null;
  }
  return { upperBound: seedUpperBound, count };
}

async function getFirstAvailableLocator(page, selectorCandidates, options = {}) {
  const state = options.state ?? 'visible';
  const timeout = options.timeout ?? 5000;
  const useLastMatch = options.useLastMatch ?? false;
  const method = useLastMatch ? 'last' : 'first';
  for (const selector of selectorCandidates) {
    const locator = page.locator(selector)[method]();
    try {
      await locator.waitFor({ state, timeout });
      return locator;
    } catch {
      // try next selector
    }
  }
  throw new Error(`Unable to resolve locator for selectors: ${selectorCandidates.join(', ')}`);
}

async function ensureTextInput(baseLocator) {
  const tagName = await baseLocator.evaluate((node) => node.tagName?.toLowerCase?.());
  if (tagName === 'input' || tagName === 'textarea') {
    return baseLocator;
  }
  const child = baseLocator.locator('input,textarea').first();
  await child.waitFor({ state: 'visible', timeout: 3000 });
  return child;
}

async function ensureToggleOn(page, selector) {
  return ensureToggleState(page, { selectors: Array.isArray(selector) ? selector : [selector], desiredState: true });
}

async function ensureAiToggles(page, accordionButton) {
  await page.waitForTimeout(300);
  for (const toggle of filterSelectors.aiToggles) {
    await setToggleState(page, toggle);
  }
  console.log('AI scrubbing toggles configured.');
}

async function setToggleState(page, toggle) {
  const desired = typeof toggle.desiredState === 'boolean' ? toggle.desiredState : true;
  const stateLocator = await getFirstAvailableLocator(page, toggle.stateSelectors, {
    state: 'attached',
    timeout: 4000,
  });
  const switchLocator = await getSwitchLocator(page, toggle);
  await switchLocator.scrollIntoViewIfNeeded();
  await switchLocator.waitFor({ state: 'visible', timeout: 4000 });

  for (let attempt = 0; attempt < 6; attempt += 1) {
    const state = await getSwitchState(switchLocator, stateLocator);
    if (state === desired) break;
    await stateLocator.evaluate((node) => node.click());
    await page.waitForTimeout(250);
  }

  let finalState = await getSwitchState(switchLocator, stateLocator);
  if (finalState !== desired) {
    await stateLocator.evaluate((node) => node.scrollIntoView({ block: 'center', behavior: 'smooth' }));
    await page.waitForTimeout(300);
    for (let attempt = 0; attempt < 3; attempt += 1) {
      finalState = await getSwitchState(switchLocator, stateLocator);
      if (finalState === desired) break;
    await stateLocator.evaluate((node) => node.click());
      await page.waitForTimeout(250);
    }
  }

  finalState = await getSwitchState(switchLocator, stateLocator);
  if (finalState !== desired) {
    console.warn(`Toggle "${toggle.name}" may remain ${finalState ? 'enabled' : 'disabled'}; expected ${desired}.`);
  } else {
    console.log(`Toggle "${toggle.name}" set to ${desired ? 'on' : 'off'}.`);
  }
}

async function toggleSwitch(page, toggleConfig, desired) {
  const { stateSelectors = [], controlSelectors = [] } =
    Array.isArray(toggleConfig) ? { stateSelectors: toggleConfig, controlSelectors: toggleConfig } : toggleConfig;

  const stateLocator = await getFirstAvailableLocator(page, stateSelectors, {
    state: 'attached',
    timeout: 4000,
  });
  const controlLocator = await getFirstAvailableLocator(page, controlSelectors.length ? controlSelectors : stateSelectors, {
    state: 'visible',
    timeout: 4000,
  });

  await controlLocator.scrollIntoViewIfNeeded();
  for (let attempt = 0; attempt < 3; attempt += 1) {
    const current = await stateLocator.evaluate((node) => node.checked);
    if (current === desired) break;
    await controlLocator.click({ delay: 50, force: true });
    await page.waitForTimeout(250);
  }
  const finalState = await stateLocator.evaluate((node) => node.checked);
  if (finalState !== desired) {
    console.warn(`Switch ${controlSelectors[0] || stateSelectors[0]} may remain ${finalState ? 'on' : 'off'}.`);
  } else {
    console.log(`Switch ${controlSelectors[0] || stateSelectors[0]} set to ${finalState ? 'on' : 'off'}.`);
  }
}

async function clickConfirmPurchaseButton(page, options = {}) {
  const timeout = options.timeout ?? 8000;
  const dialog = await resolveConfirmPurchaseDialog(page, timeout);
  const dialogId = await dialog.getAttribute('id').catch(() => null);
  if (dialogId) {
    console.log(`Confirm Purchase dialog detected (id=${dialogId}).`);
  }
  let purchaseButton;
  try {
    purchaseButton = await resolvePurchaseButton(dialog, timeout);
  } catch (error) {
    const markupSample = (
      (await dialog.innerHTML().catch(() => '')) || ''
    )
      .replace(/\s+/g, ' ')
      .trim()
      .slice(0, 600);
    console.warn(`Unable to resolve Purchase button from dialog markup sample: ${markupSample}`);
    throw error;
  }
  await purchaseButton.scrollIntoViewIfNeeded();
  await purchaseButton.hover().catch(() => {});
  await purchaseButton.click({ delay: 60, force: true });
  await dialog.waitFor({ state: 'hidden', timeout: 5000 });
  console.log('Clicked Confirm purchase button.');
}

async function findLocatorOrNull(page, selectors, options = {}) {
  try {
    return await getFirstAvailableLocator(page, selectors, options);
  } catch {
    return null;
  }
}

async function resolveConfirmPurchaseDialog(page, timeout) {
  const selectors = [
    '[data-floating-ui-portal] div[role="dialog"]',
    '[data-floating-ui-portal] .lui-modal',
    'div[role="dialog"]',
  ];
  let lastError;
  for (const selector of selectors) {
    const candidate = page.locator(selector).filter({ hasText: /Confirm Purchase/i }).last();
    try {
      await candidate.waitFor({ state: 'visible', timeout });
      return candidate;
    } catch (error) {
      lastError = error;
    }
  }
  console.warn(`Selector-based confirm dialog lookup failed (${lastError?.message ?? 'unknown error'}).`);
  return getFirstAvailableLocator(page, exportFlowSelectors.confirmPurchaseDialog, {
    state: 'visible',
    timeout,
  });
}

async function resolvePurchaseButton(dialog, timeout) {
  const selectors = [
    'button.lui-button-primary:has-text("Purchase")',
    'button.lui-button--primary:has-text("Purchase")',
    'button:has-text("Purchase")',
  ];
  let lastError;
  for (const selector of selectors) {
    const candidate = dialog.locator(selector).filter({
      hasText: /^Purchase$/i,
    });
    try {
      const button = candidate.first();
      await button.waitFor({ state: 'visible', timeout });
      const text = ((await button.innerText()) || '').trim();
      if (/^Purchase$/i.test(text)) {
        return button;
      }
    } catch (error) {
      lastError = error;
    }
  }
  throw new Error(`Unable to locate Purchase button inside dialog (${lastError?.message ?? 'unknown error'}).`);
}

async function getSwitchLocator(page, toggle) {
  if (toggle.switchName) {
    const roleLocator = page.getByRole('switch', { name: new RegExp(toggle.switchName, 'i') }).first();
    try {
      await roleLocator.waitFor({ state: 'visible', timeout: 2000 });
      return roleLocator;
    } catch {
      // fall through to selector-based lookup
    }
  }
  const selectors = toggle.controlSelectors || [];
  if (!selectors.length) {
    throw new Error(`No selectors provided for toggle ${toggle.name}`);
  }
  return getFirstAvailableLocator(page, selectors);
}

async function ensureAccordionOpen(page, label, fallbackSelectors, contentSelector) {
  const button = await getDropdownButton(page, label, fallbackSelectors);
  const expanded = await button.getAttribute('aria-expanded');
  if (expanded !== 'true') {
    await button.scrollIntoViewIfNeeded();
    await button.click({ delay: 50, force: true });
  }
  if (contentSelector) {
    await waitForAnySelector(page, contentSelector, { state: 'visible', timeout: 5000 }).catch(async () => {
      await waitForAnySelector(page, contentSelector, { state: 'attached', timeout: 5000 });
    });
  }
  return button;
}

async function getDropdownButton(page, label, fallbackSelectors) {
  if (fallbackSelectors?.length) {
    try {
      return await getFirstAvailableLocator(page, fallbackSelectors, { state: 'visible', timeout: 4000 });
    } catch {
      // fall through
    }
  }
  const button = page.getByRole('button', { name: new RegExp(`^\\s*${label}\\b`, 'i') }).first();
  await button.waitFor({ state: 'visible', timeout: 4000 });
  return button;
}

async function waitForAnySelector(page, selectors, options = {}) {
  const selectorList = Array.isArray(selectors) ? selectors : [selectors];
  let lastError;
  for (const selector of selectorList) {
    try {
      return await page.waitForSelector(selector, options);
    } catch (error) {
      lastError = error;
    }
  }
  throw lastError || new Error('Selectors did not resolve.');
}

async function getSwitchState(locator, stateLocator = null) {
  const target = stateLocator ?? locator;
  const state = await target.evaluate((node) => {
    const aria = node.getAttribute?.('aria-checked');
    if (aria != null) {
      return aria === 'true';
    }
    const ariaPressed = node.getAttribute?.('aria-pressed');
    if (ariaPressed != null) {
      return ariaPressed === 'true';
    }
    const dataState = node.getAttribute?.('data-state');
    if (dataState != null) {
      const normalized = typeof dataState === 'string' ? dataState.toLowerCase() : dataState;
      return ['true', 'checked', 'on', '1'].includes(normalized);
    }
    const input =
      node.closest?.('label')?.querySelector?.('input[type=\"checkbox\"],input[type=\"radio\"]') ||
      node.querySelector?.('input[type=\"checkbox\"],input[type=\"radio\"]');
    if (input) {
      return input.checked;
    }
    return null;
  });
  if (state != null) {
    return state;
  }
  if (stateLocator == null) {
    return null;
  }
  return locator.evaluate((node) => {
    const aria = node.getAttribute?.('aria-checked');
    if (aria != null) {
      return aria === 'true';
    }
    const ariaPressed = node.getAttribute?.('aria-pressed');
    if (ariaPressed != null) {
      return ariaPressed === 'true';
    }
    const dataState = node.getAttribute?.('data-state');
    if (dataState != null) {
      const normalized = typeof dataState === 'string' ? dataState.toLowerCase() : dataState;
      return ['true', 'checked', 'on', '1'].includes(normalized);
    }
    return null;
  });
}

async function selectCountySuggestion(page, targetCounty) {
  try {
    await page.waitForSelector(filterSelectors.countyMenu, { state: 'visible', timeout: 5000 });
    const suggestion = page
      .locator(filterSelectors.countyResult)
      .filter({ hasText: targetCounty })
      .first();
    await suggestion.waitFor({ state: 'visible', timeout: 3000 });
    await suggestion.click();
    await page.waitForTimeout(200);
    return;
  } catch (error) {
    console.warn('County suggestion click failed, falling back to keyboard selection.', error.message);
  }
  await page.keyboard.press('ArrowDown');
  await page.keyboard.press('Enter');
}

function parseNumeric(value, fallback) {
  if (value == null || value === '') {
    return fallback;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function normalizeIncrement(value) {
  if (!Number.isFinite(value) || value <= 0) {
    return 0.01;
  }
  const normalized = Math.max(value, 0.0001);
  return Math.min(normalized, 1);
}

async function run() {
  const browser = await chromium.launch({ headless, slowMo });
  const context = await browser.newContext({
    acceptDownloads: true,
  });
  if (remainingTokenBudget != null) {
    console.log(
      `Token budget configured for ${remainingTokenBudget.toLocaleString()} parcel${
        remainingTokenBudget === 1 ? '' : 's'
      }. Exports exceeding the balance will be skipped.`
    );
  }

  const page = await context.newPage();
  console.log('Navigating to Land Insights login...');
  await page.goto('https://app.landinsights.co/', { waitUntil: 'domcontentloaded' });

  await page.waitForSelector(selectors.email, { timeout: 15000 });
  await page.fill(selectors.email, email);
  console.log('Email filled');

  await page.fill(selectors.password, password);
  console.log('Password filled');

  const submitButton = page.locator(selectors.submit);
  await Promise.all([
    page.waitForURL(/app\.landinsights\.co\/home/i, { timeout: 45000 }),
    submitButton.click({ delay: 150 }),
  ]);
  console.log('Dashboard detected, proceeding with navigation.');

  for (const step of navigationSteps) {
    await clickFirstMatch(page, step);
  }
  console.log('Reached Filter & Export Parcels flow.');
  await fillFilterForm(page);

  if (!autoMode) {
    console.log('Browser ready. Perform any additional actions, then press ENTER to finish.');
    await new Promise((resolve) => {
      process.stdin.resume();
      process.stdin.once('data', resolve);
    });
  } else if (holdMs > 0) {
    console.log(`Auto mode: keeping session open for ${holdMs}ms...`);
    await page.waitForTimeout(holdMs);
  }

  await context.close();
  await browser.close();
  console.log('Automation complete. CSV exports (if any) are saved under artifacts/downloads/.');
}

run().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
