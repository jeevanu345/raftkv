import { test, expect } from '@playwright/test';
const routes=['/','/keys','/console','/raft','/metrics','/simulation','/administration'];
test('appearance applies globally, persists, and controls stay in Administration',async({page})=>{
 await page.goto('/administration');
 await page.getByLabel('Color theme').selectOption('light');
 await page.getByRole('button',{name:'Increase font size'}).click();
 await expect(page.getByLabel('Global font size')).toHaveValue('105');
 await page.reload();
 await expect(page.getByLabel('Color theme')).toHaveValue('light');
 await expect(page.getByLabel('Global font size')).toHaveValue('105');
 for(const route of routes){
  await page.goto(route);
  await expect(page.locator('html')).toHaveAttribute('data-theme','light');
  expect(await page.evaluate(()=>getComputedStyle(document.documentElement).fontSize)).toBe('16.8px');
  if(route!='/administration')await expect(page.getByLabel('Color theme')).toHaveCount(0);
 }
 await page.getByRole('button',{name:'Decrease font size'}).click();
 await expect(page.getByLabel('Global font size')).toHaveValue('100');
 await page.getByRole('button',{name:'Reset appearance'}).click();
 await expect(page.getByLabel('Color theme')).toHaveValue('dark');
});
for(const theme of ['dark','light'])for(const width of [1440,768])test(`${theme} at largest font stays usable at ${width}`,async({page})=>{
 const errors:string[]=[];page.on('pageerror',e=>errors.push(e.message));
 await page.addInitScript(theme=>localStorage.setItem('raftkv.appearance.v1',JSON.stringify({theme,fontScale:150})),theme);
 await page.setViewportSize({width,height:1000});
 for(const route of routes){
  await page.goto(route);await expect(page.locator('.topbar__title')).toBeVisible();await page.waitForTimeout(300);
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),route).toBeTruthy();
 }
 expect(errors).toEqual([]);
});
test('long event IDs cannot overlap the icon or details',async({page})=>{
 await page.goto('/raft');const row=page.locator('.event-row').first();await expect(row).toBeVisible();
 await row.locator('.event-row__sequence').evaluate(el=>{el.textContent='#1791216827646775';});
 const id=await row.locator('.event-row__sequence').boundingBox();const icon=await row.locator('.event-row__icon').boundingBox();const content=await row.locator('.event-row__content').boundingBox();
 expect(id!.x+id!.width).toBeLessThanOrEqual(icon!.x);expect(content!.y).toBeGreaterThanOrEqual(id!.y+id!.height);
});
test('supplied project logo loads without distortion',async({page})=>{
 await page.goto('/');const logo=page.getByRole('img',{name:'RaftKV',exact:true});await expect(logo).toBeVisible();
 await expect(logo).toHaveJSProperty('naturalWidth',1254);
 await expect(logo).toHaveJSProperty('complete',true);
 const box=await logo.boundingBox();expect(box!.width).toBeCloseTo(box!.height,0);
});
