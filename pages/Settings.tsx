

import React, { useState, useEffect } from 'react';
import { Save, Database, Code, CheckCircle2, Copy, AlertCircle, Link, ExternalLink, FileSpreadsheet, RefreshCw, UploadCloud, DownloadCloud, Radio, Palette, Lock } from 'lucide-react';
import { getAppSettings, saveAppSettings, pushFullDatabase, pullFullDatabase, THEMES, getTheme, applyTheme } from '../services/sheetService';

const APP_SCRIPT_CODE = `/**
 * BACKEND SCRIPT FOR DOUBLE GOOD FARMING
 * Paste this into Google Apps Script (Extensions > Apps Script)
 */

function doPost(e) {
  const lock = LockService.getScriptLock();
  lock.tryLock(30000); // Wait up to 30s

  try {
    const json = JSON.parse(e.postData.contents);
    
    // FULL SYNC ACTION
    if (json.action === 'SYNC_FULL_DB') {
       return syncFullDatabase(json.payload);
    }
    
    // INDIVIDUAL ACTIONS (Fallback/Legacy)
    if (json.action === 'CHECK_ALERTS') return checkAlerts();
    if (json.action === 'CLEAR_ALERT') return clearAlert(json.payload);

    return response({ success: false, message: "Invalid Action" });

  } catch (e) {
    return response({ success: false, error: e.toString() });
  } finally {
    lock.releaseLock();
  }
}

function doGet(e) {
  const action = e.parameter.action;
  if (action === 'GET_FULL_DB') return getFullDatabase();
  if (action === 'CHECK_ALERTS') return checkAlerts();
  return response({ success: false, message: "Invalid Action" });
}

// --- CORE SYNC LOGIC ---

function syncFullDatabase(payload) {
  // Payload contains: { batches: [], inventory: [], finishedGoods: [], dailyCosts: [] }
  
  if (payload.batches) syncSheet("Processing_Batches", payload.batches, 
      ["ID", "Status", "Source Farm", "Date Received", "Raw Weight", "Spoiled", "Net Weight", "Wash", "Dry", "Packed Date", "Recipe", "Count", "Config"],
      (b) => [
          b.id, b.status, b.sourceFarm, b.dateReceived,
          b.rawWeightKg, b.spoiledWeightKg, b.netWeightKg,
          '', '', b.packedDate||'', b.recipeType||'', b.packCount||'',
          b.processConfig ? JSON.stringify(b.processConfig) : ''
      ]
  );

  if (payload.inventory) syncSheet("Inventory", payload.inventory,
      ["ID", "Name", "Type", "Subtype", "Quantity", "Threshold", "Unit", "UnitCost", "Supplier", "PackSize"],
      (i) => [i.id, i.name, i.type, i.subtype, i.quantity, i.threshold, i.unit, i.unitCost, i.supplier, i.packSize]
  );

  if (payload.finishedGoods) syncSheet("Finished_Goods", payload.finishedGoods,
      ["ID", "BatchID", "Recipe", "Packaging", "Quantity", "DatePacked", "SellingPrice"],
      (f) => [f.id, f.batchId, f.recipeName, f.packagingType, f.quantity, f.datePacked, f.sellingPrice]
  );
  
  // Updated Daily Costs Schema - Transactional
  if (payload.dailyCosts) syncSheet("Daily_Costs", payload.dailyCosts,
      ["ID", "Reference", "Date", "RawCost", "PkgCost", "LaborCost", "WastageCost", "TotalCost", "WeightProcessed", "Hours"],
      (c) => [c.id, c.referenceId, c.date, c.rawMaterialCost, c.packagingCost, c.laborCost, c.wastageCost, c.totalCost, c.weightProcessed, c.processingHours]
  );

  return response({ success: true, message: "Full Database Synced" });
}

function getFullDatabase() {
    return response({
        success: true,
        data: {
            batches: getSheetData("Processing_Batches"),
            inventory: getSheetData("Inventory"),
            finishedGoods: getSheetData("Finished_Goods"),
            dailyCosts: getSheetData("Daily_Costs")
        }
    });
}

// --- HELPER: SYNC SHEET GENERIC (UPSERT) ---
function syncSheet(sheetName, items, headers, rowMapper) {
   const ss = SpreadsheetApp.getActiveSpreadsheet();
   let sheet = ss.getSheetByName(sheetName);
   if (!sheet) {
      sheet = ss.insertSheet(sheetName);
      sheet.appendRow(headers);
   }
   
   // Get existing data to map IDs to Rows
   const lastRow = sheet.getLastRow();
   const data = lastRow > 1 ? sheet.getRange(2, 1, lastRow - 1, 1).getValues() : [];
   const idMap = new Map(); // ID -> Row Index (0-based from data start)
   data.forEach((r, i) => idMap.set(String(r[0]), i));
   
   const newRows = [];
   
   items.forEach(item => {
       const id = String(item.id); 
       if (idMap.has(id)) {
           // Update existing row
           const rowIndex = idMap.get(id) + 2; // +2 for header and 1-based index
           const rowValues = rowMapper(item);
           sheet.getRange(rowIndex, 1, 1, rowValues.length).setValues([rowValues]);
       } else {
           // Insert new
           newRows.push(rowMapper(item));
       }
   });
   
   if (newRows.length > 0) {
       sheet.getRange(lastRow + 1, 1, newRows.length, newRows[0].length).setValues(newRows);
   }
}

// --- HELPER: READ SHEET GENERIC ---
function getSheetData(sheetName) {
    const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(sheetName);
    if (!sheet || sheet.getLastRow() <= 1) return [];
    
    const data = sheet.getDataRange().getValues();
    data.shift(); // Remove header
    
    // Simple mapping based on sheet name
    if (sheetName === 'Processing_Batches') {
        return data.map(row => ({
           id: row[0], status: row[1], sourceFarm: row[2], dateReceived: row[3],
           rawWeightKg: Number(row[4]), spoiledWeightKg: Number(row[5]), netWeightKg: Number(row[6]),
           packedDate: row[9], 
           recipeType: row[10], 
           selectedRecipeName: row[10], // Map Column K (Recipe) to selectedRecipeName
           packCount: Number(row[11]),
           processConfig: row[12] ? JSON.parse(row[12]) : null
        }));
    }
    if (sheetName === 'Inventory') {
        return data.map(r => ({
           id: r[0], name: r[1], type: r[2], subtype: r[3],
           quantity: Number(r[4]), threshold: Number(r[5]), unit: r[6],
           unitCost: Number(r[7]), supplier: r[8], packSize: Number(r[9])
        }));
    }
    if (sheetName === 'Finished_Goods') {
         return data.map(r => ({
            id: r[0], batchId: r[1], recipeName: r[2], packagingType: r[3],
            quantity: Number(r[4]), datePacked: r[5], sellingPrice: Number(r[6])
         }));
    }
    if (sheetName === 'Daily_Costs') {
         // Updated to match new column order: ID, Ref, Date, Raw, Pkg, Labor, Wastage, Total
         return data.map(r => ({
            id: r[0],
            referenceId: r[1],
            date: r[2], 
            rawMaterialCost: Number(r[3]), 
            packagingCost: Number(r[4]),
            laborCost: Number(r[5]), 
            wastageCost: Number(r[6]), 
            totalCost: Number(r[7]),
            weightProcessed: Number(r[8]),
            processingHours: Number(r[9])
         }));
    }
    return [];
}


// --- ALERTS (MN_HARVESTS INTEGRATION) ---

function checkAlerts() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  
  // 1. Loose Sheet Finding (Case/Space Insensitive)
  const sheets = ss.getSheets();
  const sheet = sheets.find(s => s.getName().toLowerCase().replace(/[^a-z0-9]/g, '') === "mnharvests") || ss.getSheetByName("mn_harvests");
  
  if (!sheet) return response({ success: true, data: [], message: "Sheet mn_harvests not found" });

  const data = sheet.getDataRange().getValues();
  if (data.length < 2) return response({ success: true, data: [] });

  const headerRow = data[0];
  
  // 2. Helper to find column index by keywords
  function findCol(keywords) {
    return headerRow.findIndex(h => {
       const cleanH = String(h).toLowerCase().replace(/[^a-z0-9]/g, '');
       return keywords.some(k => cleanH.includes(k));
    });
  }

  const idxId = findCol(['id', 'alertid']);
  const idxEntity = findCol(['entity', 'farm', 'source', 'name']);
  const idxSpecies = findCol(['species', 'variety', 'type']);
  const idxWeight = findCol(['weight', 'qty', 'amount', 'kg']);
  const idxStatus = findCol(['status', 'transfer', 'trigger']);
  const idxDate = findCol(['date', 'time', 'created']);

  // 3. Fallback: Data Scanning
  // If status column not found by header, scan first 10 rows for "RECEIVED_AT_PROCESSING"
  let finalIdxStatus = idxStatus;
  if (finalIdxStatus === -1) {
     for(let r=1; r<Math.min(data.length, 10); r++) {
         for(let c=0; c<data[r].length; c++) {
             if(String(data[r][c]).trim().toUpperCase() === 'RECEIVED_AT_PROCESSING') {
                 finalIdxStatus = c;
                 break;
             }
         }
         if(finalIdxStatus !== -1) break;
     }
  }

  // Fallback: If ID not found, use Column A (0)
  const finalIdxId = idxId !== -1 ? idxId : 0;

  if (finalIdxStatus === -1) return response({ success: false, message: "Status column not found (Looked for 'RECEIVED_AT_PROCESSING')" });

  const alerts = [];
  for (let i = 1; i < data.length; i++) {
    const row = data[i];
    const rawStatus = String(row[finalIdxStatus]).trim().toUpperCase();
    
    if (rawStatus === 'RECEIVED_AT_PROCESSING' || rawStatus === 'READY_TO_PROCESS') {
       alerts.push({
         id: row[finalIdxId],
         farmName: idxEntity !== -1 ? row[idxEntity] : 'Unknown Farm',
         species: idxSpecies !== -1 ? row[idxSpecies] : 'Unknown Species',
         estimatedWeightKg: idxWeight !== -1 ? Number(row[idxWeight]) : 0,
         timestamp: idxDate !== -1 ? row[idxDate] : new Date().toISOString()
       });
    }
  }
  
  return response({ success: true, data: alerts });
}

function clearAlert(payload) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  // Same loose logic for finding sheet
  const sheets = ss.getSheets();
  const sheet = sheets.find(s => s.getName().toLowerCase().replace(/[^a-z0-9]/g, '') === "mnharvests") || ss.getSheetByName("mn_harvests");

  if (!sheet) return response({ success: false, message: "Sheet not found" });

  const data = sheet.getDataRange().getValues();
  
  // Same Logic: Scan for "RECEIVED_AT_PROCESSING" to find status column
  let idxStatus = -1;
  for(let r=1; r<Math.min(data.length, 10); r++) {
     for(let c=0; c<data[r].length; c++) {
         if(String(data[r][c]).trim().toUpperCase() === 'RECEIVED_AT_PROCESSING') {
             idxStatus = c;
             break;
         }
     }
     if(idxStatus !== -1) break;
  }
  
  // Fallback by header
  if (idxStatus === -1) {
      const headerRow = data[0];
      idxStatus = headerRow.findIndex(h => String(h).toLowerCase().includes('status'));
  }

  // Find ID column
  let idxId = data[0].findIndex(h => String(h).toLowerCase().includes('id'));
  if (idxId === -1) idxId = 0; // Default to col A

  if (idxStatus === -1) return response({ success: false, message: "Status column not found" });

  for (let i = 1; i < data.length; i++) {
    if (String(data[i][idxId]).trim() == String(payload.id).trim()) {
      sheet.getRange(i + 1, idxStatus + 1).setValue('BATCH_CREATED');
      return response({ success: true, message: "Alert cleared" });
    }
  }
  return response({ success: false, message: "Alert ID not found" });
}

function response(obj) {
  return ContentService.createTextOutput(JSON.stringify(obj)).setMimeType(ContentService.MimeType.JSON);
}`;

const SettingsPage: React.FC = () => {
  const [activeTab, setActiveTab] = useState<'connection' | 'appearance' | 'schema' | 'code' | 'sync'>('connection');
  const [scriptUrl, setScriptUrl] = useState('');
  const [sheetUrl, setSheetUrl] = useState('');
  const [useMock, setUseMock] = useState(true);
  const [saved, setSaved] = useState(false);
  const [copied, setCopied] = useState(false);
  const [syncStatus, setSyncStatus] = useState('');
  const [isSyncing, setIsSyncing] = useState(false);
  const [currentTheme, setCurrentTheme] = useState('mushroom');
  const [isFixed, setIsFixed] = useState(false);

  useEffect(() => {
    const settings = getAppSettings();
    setScriptUrl(settings.scriptUrl);
    setSheetUrl(settings.sheetUrl);
    setUseMock(settings.useMock);
    setIsFixed(!!settings.isFixed);
    setCurrentTheme(getTheme());
  }, []);

  const handleSave = () => {
    if (isFixed) return; // Prevent save if fixed
    let finalUseMock = useMock;
    if (scriptUrl && scriptUrl.length > 10) {
        finalUseMock = false;
        setUseMock(false);
    }
    saveAppSettings(scriptUrl, sheetUrl, finalUseMock);
    setSaved(true);
    setTimeout(() => setSaved(false), 2000);
  };

  const handleCopyCode = () => {
    navigator.clipboard.writeText(APP_SCRIPT_CODE);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const handlePushData = async () => {
    if (!scriptUrl) {
      setSyncStatus('Error: No API URL configured.');
      return;
    }
    
    setIsSyncing(true);
    setSyncStatus('Pushing full database to Sheet...');
    
    const result = await pushFullDatabase();
    
    setIsSyncing(false);
    if (result.success) {
      setSyncStatus(result.message || 'Data pushed successfully.');
    } else {
      setSyncStatus('Failed to push data. Check connection.');
    }
  };

  const handlePullData = async () => {
    if (!scriptUrl) {
      setSyncStatus('Error: No API URL configured.');
      return;
    }

    setIsSyncing(true);
    setSyncStatus('Fetching full database from Sheet...');
    
    const result = await pullFullDatabase();

    setIsSyncing(false);
    if (result.success) {
      setSyncStatus(result.message);
    } else {
      setSyncStatus('Failed to fetch data. Check your Web App URL.');
    }
  };

  const handleThemeChange = (themeId: string) => {
    setCurrentTheme(themeId);
    applyTheme(themeId);
  };

  const tabs = [
    { id: 'connection', label: 'Connection', icon: Link },
    { id: 'sync', label: 'Data Sync', icon: RefreshCw },
    { id: 'appearance', label: 'Appearance', icon: Palette },
    { id: 'schema', label: 'Database Schema', icon: Database },
    { id: 'code', label: 'Backend Script', icon: Code },
  ];

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-3xl font-bold text-slate-900">System Configuration</h2>
        <p className="text-slate-500">Manage Google Sheets integration and backend settings.</p>
      </div>

      <div className="bg-white rounded-2xl shadow-sm border border-slate-200 overflow-hidden">
        <div className="flex border-b border-slate-100 overflow-x-auto">
          {tabs.map((tab) => {
            const Icon = tab.icon;
            return (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id as any)}
                className={`flex-1 py-4 px-4 text-sm font-medium flex items-center justify-center transition-colors whitespace-nowrap ${
                  activeTab === tab.id
                    ? 'text-nature-600 border-b-2 border-nature-600 bg-nature-50/50'
                    : 'text-slate-500 hover:text-slate-700 hover:bg-slate-50'
                }`}
              >
                <Icon size={18} className="mr-2" />
                {tab.label}
              </button>
            );
          })}
        </div>

        <div className="p-6 md:p-8">
          {activeTab === 'connection' && (
            <div className="space-y-6 max-w-2xl">
              <div className="p-4 bg-blue-50 text-blue-800 rounded-lg flex items-start text-sm">
                <AlertCircle size={20} className="mr-2 flex-shrink-0 mt-0.5" />
                <p>Ensure your Google Apps Script is deployed as a Web App with access set to "Anyone".</p>
              </div>

              {isFixed && (
                <div className="p-3 bg-nature-100 text-nature-800 rounded-lg flex items-center text-sm font-bold">
                    <Lock size={16} className="mr-2"/> Configuration Locked in Code
                </div>
              )}

              <div className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-2">Google Apps Script Web App URL</label>
                  <input
                    type="text"
                    value={scriptUrl}
                    onChange={(e) => setScriptUrl(e.target.value)}
                    disabled={isFixed}
                    placeholder="https://script.google.com/macros/s/..."
                    className={`w-full rounded-lg border-slate-300 border p-3 focus:ring-2 focus:ring-nature-500 font-mono text-sm ${isFixed ? 'bg-slate-100 text-slate-500 cursor-not-allowed' : ''}`}
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-2">Google Spreadsheet URL</label>
                  <div className="flex space-x-2">
                    <input
                      type="text"
                      value={sheetUrl}
                      onChange={(e) => setSheetUrl(e.target.value)}
                      disabled={isFixed}
                      placeholder="https://docs.google.com/spreadsheets/d/..."
                      className={`flex-1 rounded-lg border-slate-300 border p-3 focus:ring-2 focus:ring-nature-500 font-mono text-sm ${isFixed ? 'bg-slate-100 text-slate-500 cursor-not-allowed' : ''}`}
                    />
                    {sheetUrl && (
                      <a 
                        href={sheetUrl} 
                        target="_blank" 
                        rel="noopener noreferrer"
                        className="px-4 bg-slate-100 text-slate-600 rounded-lg flex items-center justify-center hover:bg-slate-200"
                      >
                        <ExternalLink size={20} />
                      </a>
                    )}
                  </div>
                </div>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div className="flex items-center space-x-3 bg-slate-50 p-4 rounded-lg border border-slate-200">
                    <div className="flex-1">
                      <span className="block font-medium text-slate-800">Use Mock Data</span>
                      <span className="text-xs text-slate-500">Enable for demo/testing without Google Sheets</span>
                    </div>
                    <button
                      onClick={() => setUseMock(!useMock)}
                      className={`relative inline-flex h-6 w-11 items-center rounded-full transition-colors ${
                        useMock ? 'bg-nature-500' : 'bg-slate-300'
                      }`}
                    >
                      <span className={`inline-block h-4 w-4 transform rounded-full bg-white transition-transform ${useMock ? 'translate-x-6' : 'translate-x-1'}`} />
                    </button>
                  </div>
              </div>

              {!isFixed && (
                <button onClick={handleSave} className="px-6 py-3 bg-earth-800 text-white rounded-lg font-medium flex items-center">
                    {saved ? <CheckCircle2 size={18} className="mr-2" /> : <Save size={18} className="mr-2" />}
                    {saved ? 'Settings Saved' : 'Save Configuration'}
                </button>
              )}
            </div>
          )}

          {activeTab === 'sync' && (
             <div className="space-y-6 max-w-2xl">
               <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                 <div className="p-6 border border-slate-200 rounded-xl hover:border-nature-500 transition-colors">
                   <div className="bg-blue-50 w-12 h-12 rounded-full flex items-center justify-center text-blue-600 mb-4"><UploadCloud size={24} /></div>
                   <h3 className="font-bold text-slate-900 mb-2">Push to Central DB</h3>
                   <p className="text-xs text-slate-500 mb-4">Sends ALL local data (Batches, Inventory, Costs) to Google Sheet.</p>
                   <button onClick={handlePushData} disabled={isSyncing} className="w-full py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 font-medium disabled:opacity-50">
                     {isSyncing ? 'Syncing...' : 'Push Data Now'}
                   </button>
                 </div>
                 <div className="p-6 border border-slate-200 rounded-xl hover:border-nature-500 transition-colors">
                   <div className="bg-purple-50 w-12 h-12 rounded-full flex items-center justify-center text-purple-600 mb-4"><DownloadCloud size={24} /></div>
                   <h3 className="font-bold text-slate-900 mb-2">Pull from Central DB</h3>
                   <p className="text-xs text-slate-500 mb-4">Overwrites local data with latest data from Google Sheet.</p>
                   <button onClick={handlePullData} disabled={isSyncing} className="w-full py-2 bg-white border border-slate-300 text-slate-700 rounded-lg hover:bg-slate-50 font-medium disabled:opacity-50">
                     {isSyncing ? 'Connecting...' : 'Pull Data Now'}
                   </button>
                 </div>
               </div>
               {syncStatus && <div className="p-4 bg-slate-800 text-white rounded-lg flex items-center"><RefreshCw size={18} className={`mr-3 ${isSyncing ? 'animate-spin' : ''}`} /><span className="font-mono text-sm">{syncStatus}</span></div>}
             </div>
          )}

          {activeTab === 'appearance' && (
            <div className="space-y-6 max-w-4xl">
               <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                  {/* Cast Object.values(THEMES) to any[] to avoid type errors */}
                  {(Object.values(THEMES) as any[]).map((theme: any) => (
                    <button
                      key={theme.id}
                      onClick={() => handleThemeChange(theme.id)}
                      className={`relative overflow-hidden rounded-xl border-2 transition-all text-left group ${
                        currentTheme === theme.id ? 'border-nature-500 ring-2 ring-nature-200 shadow-lg' : 'border-slate-200 hover:border-slate-300'
                      }`}
                    >
                      <div className="h-24 w-full flex">
                         {/* Preview Palette */}
                         <div className="flex-1" style={{ backgroundColor: theme.colors['--earth-800'] }}></div>
                         <div className="flex-1" style={{ backgroundColor: theme.colors['--earth-500'] }}></div>
                         <div className="flex-1" style={{ backgroundColor: theme.colors['--nature-500'] }}></div>
                         <div className="flex-1" style={{ backgroundColor: theme.colors['--earth-100'] }}></div>
                      </div>
                      <div className="p-4 flex justify-between items-center bg-white">
                         <span className="font-bold text-slate-800">{theme.label}</span>
                         {currentTheme === theme.id && <CheckCircle2 size={20} className="text-nature-600" />}
                      </div>
                    </button>
                  ))}
               </div>
               <div className="bg-slate-50 p-4 rounded-lg text-sm text-slate-500 flex items-start">
                  <Palette size={18} className="mr-2 mt-0.5" />
                  <p>Themes adjust the primary "Earth" (neutrals/backgrounds) and "Nature" (accents/actions) color palettes across the entire application.</p>
               </div>
            </div>
          )}

          {activeTab === 'schema' && (
            <div>
               <div className="bg-yellow-50 p-4 rounded-lg mb-6 border border-yellow-200">
                   <h4 className="font-bold text-yellow-800 mb-2">Required Columns for mn_harvests</h4>
                   <p className="text-sm text-yellow-700 mb-3">Ensure your sheet has these exact column headers (case-insensitive):</p>
                   <ul className="list-disc list-inside text-sm text-yellow-800 space-y-1">
                       <li><b>id</b> - Unique Alert ID</li>
                       <li><b>entityId</b> - Farm Name</li>
                       <li><b>species</b> - Mushroom Variety</li>
                       <li><b>weightQty</b> - Estimated Weight</li>
                       <li><b>transferStatus</b> - Trigger (Must be "RECEIVED_AT_PROCESSING")</li>
                   </ul>
               </div>

              <div className="overflow-hidden border border-slate-200 rounded-lg">
                <table className="min-w-full divide-y divide-slate-200">
                  <thead className="bg-slate-50">
                    <tr><th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase">Col Index</th><th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase">Header Name</th><th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase">Description</th></tr>
                  </thead>
                  <tbody className="bg-white divide-y divide-slate-200 text-sm">
                    <tr><td className="px-6 py-4 text-slate-400">A</td><td className="px-6 py-4 font-mono font-bold text-slate-700">id</td><td className="px-6 py-4 text-slate-600">Batch ID</td></tr>
                    <tr><td className="px-6 py-4 text-slate-400">B</td><td className="px-6 py-4 font-mono font-bold text-slate-700">entityId</td><td className="px-6 py-4 text-slate-600">Farm Name</td></tr>
                    <tr><td className="px-6 py-4 text-slate-400">D</td><td className="px-6 py-4 font-mono font-bold text-slate-700">species</td><td className="px-6 py-4 text-slate-600">Mushroom Variety</td></tr>
                    <tr><td className="px-6 py-4 text-slate-400">E</td><td className="px-6 py-4 font-mono font-bold text-slate-700">weightQty</td><td className="px-6 py-4 text-slate-600">Incoming quantity</td></tr>
                    <tr><td className="px-6 py-4 text-slate-400">I</td><td className="px-6 py-4 font-mono font-bold text-slate-700">transferStatus</td><td className="px-6 py-4 text-slate-600">Trigger ("RECEIVED_AT_PROCESSING")</td></tr>
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {activeTab === 'code' && (
            <div className="relative bg-slate-900 rounded-xl overflow-hidden shadow-inner">
               <textarea readOnly value={APP_SCRIPT_CODE} className="w-full h-96 p-4 bg-slate-900 text-slate-300 font-mono text-sm focus:outline-none resize-none" />
               <button onClick={handleCopyCode} className="absolute top-4 right-4 bg-white/10 hover:bg-white/20 text-white px-3 py-1 rounded text-xs">{copied ? 'Copied' : 'Copy'}</button>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default SettingsPage;
