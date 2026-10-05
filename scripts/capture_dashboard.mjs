// Capture the running services through headless browser tests, without UI automation tools.
import { createRequire } from 'node:module';
import { mkdir, copyFile, writeFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const require=createRequire(path.join(root,'ui/dashboard/package.json'));
const {chromium}=require('@playwright/test');
const destination=process.env.RAFTKV_SCREENSHOT_DIR??path.join(root,'docs/screenshots');
const url=process.env.RAFTKV_DASHBOARD_URL??'http://127.0.0.1:5173';
const browser=await chromium.launch({headless:true,...(process.env.CHROME_PATH?{executablePath:process.env.CHROME_PATH}:{})});
const routes=[['/','cluster-overview'],['/keys','key-explorer'],['/console','command-console'],['/raft','raft-visualizer'],['/metrics','metrics'],['/simulation','simulation-lab'],['/administration','administration']];
const evidence=[];
try {
 for(const theme of (process.env.RAFTKV_SCREENSHOT_THEMES??'light').split(',')) {
  const context=await browser.newContext({viewport:{width:1600,height:1100},deviceScaleFactor:1});
  const page=await context.newPage();const errors=[];page.on('pageerror',error=>errors.push(error.message));
  await page.goto(`${url}/administration`);await page.getByLabel('Global font size').waitFor();
  if(await page.getByLabel('Global font size').inputValue()!=='150')throw new Error('Fresh browser must default to 150%');
  if(theme==='light'){await page.getByRole('combobox',{name:'Color theme'}).click();await page.getByRole('option',{name:'Light mode',exact:true}).click();}
  const folder=theme==='light'?destination:path.join(destination,'dark');await mkdir(folder,{recursive:true});
  for(const [route,name]of routes){
   await page.goto(`${url}${route}`);await page.locator('.topbar__title').waitFor();await page.waitForTimeout(800);
   if(route==='/keys'){
    const keys=page.locator('.key-list-item');if(await keys.count()){await keys.first().click();await page.waitForTimeout(500);}
   }
   if(route==='/console'){
    await page.getByRole('textbox',{name:'RESP command'}).fill('INFO raft');await page.getByRole('button',{name:'Run',exact:true}).click();await page.locator('.console-entry__response').first().waitFor();
   }
   if(route==='/metrics')await page.waitForTimeout(10000);
   if(route==='/raft')await page.locator('.event-row').first().waitFor();
   if(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth))throw new Error(`Overflow: ${theme} ${route}`);
   if(await page.evaluate(()=>getComputedStyle(document.documentElement).fontSize)!=='24px')throw new Error('Font scale drifted');
   const file=path.join(folder,`${name}.png`);await page.screenshot({path:file,fullPage:true});
   evidence.push({theme,route,fontScale:150,viewport:'1600×1100',file:path.relative(root,file)});
   if(process.env.RAFTKV_SCREENSHOT_EXPORT){const exportFolder=path.join(process.env.RAFTKV_SCREENSHOT_EXPORT,theme);await mkdir(exportFolder,{recursive:true});await copyFile(file,path.join(exportFolder,`${name}.png`));}
   console.log(`${theme}: ${name}`);
  }
  if(errors.length)throw new Error(errors.join('\n'));await context.close();
 }
 await writeFile(path.join(destination,'capture.json'),JSON.stringify({capturedAt:new Date().toISOString(),source:'live local three-node RaftKV plus isolated simulation lab',screens:evidence},null,2)+'\n');
}finally{await browser.close();}
