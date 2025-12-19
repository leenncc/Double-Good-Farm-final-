/**
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
  // Payload contains: { batches: [], inventory: [], finishedGoods: [], dailyCosts: [], customers: [] }
  
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
  
  if (payload.dailyCosts) syncSheet("Daily_Costs", payload.dailyCosts,
      ["ID", "Reference", "Date", "RawCost", "PkgCost", "LaborCost", "WastageCost", "TotalCost", "WeightProcessed", "Hours"],
      (c) => [c.id, c.referenceId, c.date, c.rawMaterialCost, c.packagingCost, c.laborCost, c.wastageCost, c.totalCost, c.weightProcessed, c.processingHours]
  );

  if (payload.customers) syncSheet("Customers", payload.customers,
      ["ID", "Name", "Contact", "Email", "Address", "Type", "Status", "Notes", "JoinDate"],
      (c) => [c.id, c.name, c.contact, c.email, c.address, c.type, c.status, c.notes||'', c.joinDate]
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
            dailyCosts: getSheetData("Daily_Costs"),
            customers: getSheetData("Customers")
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
   
   const lastRow = sheet.getLastRow();
   const data = lastRow > 1 ? sheet.getRange(2, 1, lastRow - 1, 1).getValues() : [];
   const idMap = new Map();
   data.forEach((r, i) => idMap.set(String(r[0]), i));
   
   const newRows = [];
   
   items.forEach(item => {
       const id = String(item.id); 
       if (idMap.has(id)) {
           const rowIndex = idMap.get(id) + 2;
           const rowValues = rowMapper(item);
           sheet.getRange(rowIndex, 1, 1, rowValues.length).setValues([rowValues]);
       } else {
           newRows.push(rowMapper(item));
       }
   });
   
   if (newRows.length > 0) {
       sheet.getRange(sheet.getLastRow() + 1, 1, newRows.length, newRows[0].length).setValues(newRows);
   }
}

// --- HELPER: READ SHEET GENERIC ---
function getSheetData(sheetName) {
    const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(sheetName);
    if (!sheet || sheet.getLastRow() <= 1) return [];
    
    const data = sheet.getDataRange().getValues();
    data.shift(); // Remove header
    
    if (sheetName === 'Processing_Batches') {
        return data.map(row => ({
           id: row[0], status: row[1], sourceFarm: row[2], dateReceived: row[3],
           rawWeightKg: Number(row[4]), spoiledWeightKg: Number(row[5]), netWeightKg: Number(row[6]),
           packedDate: row[9], recipeType: row[10], selectedRecipeName: row[10],
           packCount: Number(row[11]), processConfig: row[12] ? JSON.parse(row[12]) : null
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
         return data.map(r => ({
            id: r[0], referenceId: r[1], date: r[2], 
            rawMaterialCost: Number(r[3]), packagingCost: Number(r[4]),
            laborCost: Number(r[5]), wastageCost: Number(r[6]), 
            totalCost: Number(r[7]), weightProcessed: Number(r[8]), processingHours: Number(r[9])
         }));
    }
    if (sheetName === 'Customers') {
        return data.map(r => ({
            id: r[0], name: r[1], contact: r[2], email: r[3], address: r[4],
            type: r[5], status: r[6], notes: r[7], joinDate: r[8]
        }));
    }
    return [];
}

function response(obj) {
  return ContentService.createTextOutput(JSON.stringify(obj)).setMimeType(ContentService.MimeType.JSON);
}