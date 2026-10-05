import {test,expect}from '@playwright/test';
import {mockCluster,mockKeyDetails}from '../src/lib/mock';
test.beforeEach(async({page})=>{await page.route('**/api/v1/**',async route=>{const path=new URL(route.request().url()).pathname;if(path==='/api/v1/events')return route.fulfill({contentType:'text/event-stream',body:''});let body:unknown=mockCluster;if(path==='/api/v1/keys')body={cursor:null,items:[],totalApproximate:0};else if(path.startsWith('/api/v1/keys/'))body=Object.values(mockKeyDetails)[0];else if(path==='/api/v1/snapshots')body=[];await route.fulfill({json:body});});});
test('live loading, empty keys and empty snapshots',async({page})=>{let release:()=>void=()=>{};const pending=new Promise<void>(resolve=>release=resolve);await page.route('**/api/v1/keys*',async route=>{await pending;await route.fulfill({json:{cursor:null,items:[],totalApproximate:0}});});await page.goto('/keys');await expect(page.getByText(/Loading keys/)).toBeVisible();release();await expect(page.getByText(/No matching keys/)).toBeVisible();await page.goto('/administration');await expect(page.getByText('No snapshots retained.')).toBeVisible();});
test('live API failures are visible',async({page})=>{await page.route('**/api/v1/cluster',route=>route.fulfill({status:503,json:{message:'Quorum unavailable'}}));await page.goto('/');await expect(page.getByText('Quorum unavailable').first()).toBeVisible({timeout:15000});});
test('member removal requires confirmation',async({page})=>{await page.goto('/administration');await page.getByRole('button',{name:'Remove',exact:true}).first().click();await expect(page.getByRole('dialog')).toBeVisible();await page.getByRole('button',{name:'Cancel',exact:true}).click();await expect(page.getByRole('dialog')).not.toBeVisible();});
test('appearance stays available when the cluster is unavailable',async({page})=>{
 await page.route('**/api/v1/cluster',route=>route.fulfill({status:503,json:{message:'Quorum unavailable'}}));
 await page.goto('/administration');
 await expect(page.getByText('Quorum unavailable').first()).toBeVisible({timeout:15000});
 await page.getByRole('combobox',{name:'Color theme'}).click();
 await page.getByRole('option',{name:'Light mode',exact:true}).click();
 await page.getByRole('button',{name:'Increase font size'}).click();
 await expect(page.getByLabel('Global font size')).toHaveValue('105');
 await expect(page.locator('html')).toHaveAttribute('data-theme','light');
});
