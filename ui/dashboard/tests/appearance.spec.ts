import { test, expect } from '@playwright/test';
const routes=['/','/keys','/console','/raft','/metrics','/simulation','/administration'];
test('appearance applies globally, persists, and controls stay in Administration',async({page})=>{
 await page.goto('/administration');
 await page.getByRole('combobox',{name:'Color theme'}).click();
 await page.getByRole('option',{name:'Light mode',exact:true}).click();
 await page.getByRole('button',{name:'Increase font size'}).click();
 await expect(page.getByLabel('Global font size')).toHaveValue('105');
 await page.reload();
 await expect(page.getByRole('combobox',{name:'Color theme'})).toContainText('Light mode');
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
 await expect(page.getByRole('combobox',{name:'Color theme'})).toContainText('Dark mode');
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

test('custom dropdown keyboard, escape, outside click and no native selects',async({page})=>{
 await page.goto('/administration');const control=page.getByRole('combobox',{name:'Color theme'});
 await control.focus();await control.press('ArrowDown');await expect(page.getByRole('listbox')).toBeVisible();await control.press('End');await control.press('Enter');await expect(control).toContainText('Light mode');
 await control.click();await control.press('Escape');await expect(page.getByRole('listbox')).toHaveCount(0);await expect(control).toBeFocused();
 await control.click();await page.getByRole('heading',{name:'Appearance',exact:true}).click();await expect(page.getByRole('listbox')).toHaveCount(0);
 for(const route of routes){await page.goto(route);await expect(page.locator('.topbar__title')).toBeVisible();expect(await page.locator('select').count()).toBe(0);}
});
test('custom menus support command targets and membership roles',async({page})=>{
 await page.goto('/console');const target=page.getByRole('combobox',{name:'Command target'});await target.click();await page.getByRole('option',{name:/^Node 1 \(.*\)$/}).click();await expect(target).toContainText('Node 1');
 await page.goto('/administration');await page.getByRole('button',{name:'Add member',exact:true}).click();const role=page.getByRole('combobox',{name:'Member role'});await role.click();await page.getByRole('option',{name:'Promote existing learner',exact:true}).click();await expect(role).toContainText('Promote existing learner');
});
