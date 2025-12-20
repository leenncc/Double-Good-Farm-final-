import { MushroomBatch, BatchStatus, ApiResponse, InventoryItem, PurchaseOrder, SalesRecord, Customer, FinishedGood, SalesStatus, PaymentMethod, DailyCostMetrics, Supplier, Recipe, UserRole, Budget } from '../types';
import { db, auth, storage } from './firebase';
import firebase from 'firebase/compat/app';

// ============================================================================
// CONFIGURATION MANAGEMENT
// ============================================================================
const FIXED_SCRIPT_URL = ''; 
const FIXED_SHEET_URL = '';
const STORAGE_KEY_URL = 'shroomtrack_api_url';
const STORAGE_KEY_SHEET_URL = 'shroomtrack_sheet_url';
const STORAGE_KEY_MOCK = 'shroomtrack_use_mock';
const STORAGE_KEY_LABOR_RATE = 'shroomtrack_labor_rate';
const STORAGE_KEY_RAW_RATE = 'shroomtrack_raw_rate';
const STORAGE_KEY_THEME = 'shroomtrack_theme';

export const getAppSettings = () => {
  const storedUrl = localStorage.getItem(STORAGE_KEY_URL);
  const storedSheetUrl = localStorage.getItem(STORAGE_KEY_SHEET_URL);
  const storedMock = localStorage.getItem(STORAGE_KEY_MOCK);
  return {
    scriptUrl: FIXED_SCRIPT_URL || storedUrl || '',
    sheetUrl: FIXED_SHEET_URL || storedSheetUrl || '',
    useMock: storedMock === 'true',
    isFixed: FIXED_SCRIPT_URL !== ''
  };
};

export const saveAppSettings = (url: string, sheetUrl: string, useMock: boolean) => {
  localStorage.setItem(STORAGE_KEY_URL, url);
  localStorage.setItem(STORAGE_KEY_SHEET_URL, sheetUrl);
  localStorage.setItem(STORAGE_KEY_MOCK, String(useMock));
};

export const getLaborRate = (): number => parseFloat(localStorage.getItem(STORAGE_KEY_LABOR_RATE) || '12.50');
export const setLaborRate = (rate: number) => localStorage.setItem(STORAGE_KEY_LABOR_RATE, rate.toString());
export const getRawMaterialRate = (): number => parseFloat(localStorage.getItem(STORAGE_KEY_RAW_RATE) || '8.00');
export const setRawMaterialRate = (rate: number) => localStorage.setItem(STORAGE_KEY_RAW_RATE, rate.toString());

const cleanFirestoreData = <T>(data: T): T => JSON.parse(JSON.stringify(data));

// ============================================================================
// CORE DATA SERVICES
// ============================================================================

export const getUserRole = async (): Promise<string> => {
  const user = auth.currentUser;
  if (!user) return 'GUEST';
  try {
    const snap = await db.collection('user_roles').doc(user.uid).get();
    return snap.exists ? (snap.data()?.role || 'GUEST') : 'GUEST';
  } catch (error) { return 'GUEST'; }
};

// --- CUSTOMERS (CRM) ---

let mockCustomers: Customer[] = [];

export const getCustomers = async (forceRemote = false): Promise<ApiResponse<Customer[]>> => {
  try {
    const snap = await db.collection('customers').get();
    const data = snap.docs.map(d => d.data() as Customer);
    mockCustomers = data; // Sync local cache
    return { success: true, data };
  } catch (e) {
    console.warn("Using local customer cache due to permissions/connection.");
    return { success: true, data: mockCustomers };
  }
};

export const addCustomer = async (customer: Customer): Promise<ApiResponse<Customer>> => {
  const docRef = db.collection('customers').doc(customer.id);
  await docRef.set(cleanFirestoreData(customer));
  if (!mockCustomers.find(c => c.id === customer.id)) mockCustomers.push(customer);
  return { success: true, data: customer };
};

export const updateCustomer = async (id: string, updates: Partial<Customer>): Promise<boolean> => {
    try {
        await db.collection('customers').doc(id).set(updates, { merge: true });
        const idx = mockCustomers.findIndex(c => c.id === id);
        if (idx !== -1) mockCustomers[idx] = { ...mockCustomers[idx], ...updates };
        return true;
    } catch (e) { return false; }
};

export const getCustomerStats = async (customerId: string) => {
    const salesRes = await getSales();
    const customerSales = (salesRes.data || []).filter(s => s.customerId === customerId);
    const totalSpent = customerSales.reduce((sum, s) => sum + s.totalAmount, 0);
    const orderCount = customerSales.length;
    const lastOrder = [...customerSales].sort((a,b) => new Date(b.dateCreated).getTime() - new Date(a.dateCreated).getTime())[0];
    return { 
        totalSpent, 
        orderCount, 
        lastOrderDate: lastOrder ? lastOrder.dateCreated : 'Never',
        salesHistory: [...customerSales].sort((a,b) => new Date(b.dateCreated).getTime() - new Date(a.dateCreated).getTime())
    };
};

// --- SALES & ORDERS ---

let mockSales: SalesRecord[] = [];

export const getSales = async (forceRemote = false): Promise<ApiResponse<SalesRecord[]>> => {
  try {
    const snap = await db.collection('sales').orderBy('dateCreated', 'desc').get();
    const data = snap.docs.map(d => d.data() as SalesRecord);
    mockSales = data;
    return { success: true, data };
  } catch (e) { return { success: true, data: mockSales }; }
};

/**
 * deductStockAggregate (Helper):
 * Deducts requested quantity from available batches of the same product (Recipe + Packaging)
 * following a FIFO approach (oldest datePacked first).
 */
async function deductStockAggregate(recipeName: string, packagingType: string, requestedQty: number) {
    const goodsSnap = await db.collection('finished_goods')
        .where('recipeName', '==', recipeName)
        .where('packagingType', '==', packagingType)
        .get();
    
    const batches = goodsSnap.docs
        .map(doc => ({ id: doc.id, ...(doc.data() as FinishedGood) }))
        .filter(g => g.quantity > 0)
        .sort((a, b) => new Date(a.datePacked).getTime() - new Date(b.datePacked).getTime());

    const totalAvailable = batches.reduce((sum, b) => sum + b.quantity, 0);
    
    if (totalAvailable < requestedQty) {
        throw new Error(`Insufficient aggregate stock for ${recipeName} (${packagingType}). Total available: ${totalAvailable}, Requested: ${requestedQty}`);
    }

    let remainingToDeduct = requestedQty;
    const updatePromises: Promise<any>[] = [];

    for (const batch of batches) {
        if (remainingToDeduct <= 0) break;
        
        const deduct = Math.min(batch.quantity, remainingToDeduct);
        updatePromises.push(
            db.collection('finished_goods').doc(batch.id).update({
                quantity: firebase.firestore.FieldValue.increment(-deduct)
            })
        );
        remainingToDeduct -= deduct;
    }

    await Promise.all(updatePromises);
}

/**
 * createSale (FIXED):
 * 1. Implements Aggregate Stock Deduction across multiple batches.
 * 2. Enriches items with recipeName and packagingType to fix POS display issues.
 */
export const createSale = async (customerId: string, items: any[], paymentMethod: PaymentMethod, initialStatus: SalesStatus = 'INVOICED'): Promise<ApiResponse<SalesRecord>> => {
  try {
    const customerRes = await getCustomers();
    const customer = (customerRes.data || []).find(c => c.id === customerId);
    
    // Fetch finished goods for product identification
    const goodsRes = await getFinishedGoods();
    const allGoods = goodsRes.data || [];

    // Step 1: Stock Deduction (Aggregate Logic)
    for (const sellItem of items) {
        const sample = allGoods.find(g => g.id === sellItem.finishedGoodId);
        if (!sample) throw new Error(`Product reference ${sellItem.finishedGoodId} not found.`);
        
        // This helper validates total stock across ALL batches and deducts FIFO
        await deductStockAggregate(sample.recipeName, sample.packagingType, sellItem.quantity);
    }

    // Step 2: Prepare Enriched Items for Sale Record
    const enrichedItems = items.map(sellItem => {
        const detail = allGoods.find(g => g.id === sellItem.finishedGoodId);
        return {
          finishedGoodId: sellItem.finishedGoodId,
          recipeName: detail?.recipeName || 'Product',
          packagingType: detail?.packagingType || 'N/A',
          quantity: sellItem.quantity,
          unitPrice: sellItem.unitPrice
        };
    });

    // Step 3: Create Sale Record
    const newSale: SalesRecord = {
      id: `SALE-${Date.now()}`,
      invoiceId: `INV-${Math.floor(Math.random() * 100000)}`,
      customerId,
      customerName: customer ? customer.name : 'Unknown',
      items: enrichedItems,
      totalAmount: enrichedItems.reduce((sum, i) => sum + (i.quantity * i.unitPrice), 0),
      paymentMethod,
      status: initialStatus,
      dateCreated: new Date().toISOString()
    };

    await db.collection('sales').doc(newSale.id).set(cleanFirestoreData(newSale));

    return { success: true, data: newSale };
  } catch (error: any) {
    return { success: false, message: error.message };
  }
};

/**
 * submitOnlineOrder (FIXED):
 * 1. Automatically links to CRM.
 * 2. Implements Aggregate Stock Deduction across batches.
 */
export const submitOnlineOrder = async (customerName: string, customerPhone: string, customerEmail: string, customerAddress: string, cartItems: { item: FinishedGood, qty: number }[]): Promise<ApiResponse<string>> => {
    const newSaleId = `ORDER-${Date.now()}`;
    let customerId = 'GUEST';
    
    try {
        const customersRef = db.collection('customers');
        let existingDoc = null;
        let existingData = null;

        if (customerEmail) {
            const emailSnap = await customersRef.where('email', '==', customerEmail).limit(1).get();
            if (!emailSnap.empty) { existingDoc = emailSnap.docs[0]; existingData = existingDoc.data() as Customer; }
        }

        if (!existingDoc && customerPhone) {
            const phoneSnap = await customersRef.where('contact', '==', customerPhone).limit(1).get();
            if (!phoneSnap.empty) { existingDoc = phoneSnap.docs[0]; existingData = existingDoc.data() as Customer; }
        }

        if (existingDoc && existingData) {
            customerId = existingData.id;
            const updates: Partial<Customer> = {};
            if (!existingData.type) updates.type = 'B2C';
            if (!existingData.address && customerAddress) updates.address = customerAddress;
            if (Object.keys(updates).length > 0) {
                try { await existingDoc.ref.update(updates); } catch(e) { console.warn("Public profile update restricted."); }
            }
        } else {
            const newId = `cust-shop-${Date.now()}`;
            const newCust: Customer = { 
                id: newId, name: customerName, email: customerEmail, contact: customerPhone, 
                address: customerAddress, type: 'B2C', status: 'ACTIVE', joinDate: new Date().toISOString() 
            };
            
            try {
                await customersRef.doc(newId).set(cleanFirestoreData(newCust));
                customerId = newId;
            } catch (e) {
                console.warn("Public profile creation restricted (Firebase Rules). Proceeding as Guest.");
                customerId = `GUEST-${Date.now()}`;
            }
        }
    } catch (e) { console.error("CRM Auto-link error:", e); }

    try { 
        // Step 1: Stock Deduction (Aggregate Logic)
        for (const cartItem of cartItems) {
            await deductStockAggregate(cartItem.item.recipeName, cartItem.item.packagingType, cartItem.qty);
        }

        // Step 2: Prepare Sale Record
        const saleRecord: SalesRecord = {
            id: newSaleId, invoiceId: `WEB-${Math.floor(Math.random() * 10000)}`, 
            customerId, customerName, customerPhone, customerEmail, shippingAddress: customerAddress, 
            items: cartItems.map(c => ({ 
                finishedGoodId: c.item.id, recipeName: c.item.recipeName, 
                packagingType: c.item.packagingType, quantity: c.qty, unitPrice: c.item.sellingPrice 
            })), 
            totalAmount: cartItems.reduce((sum, c) => sum + ((c.item.sellingPrice || 15) * c.qty), 0), 
            paymentMethod: 'COD', status: 'QUOTATION', dateCreated: new Date().toISOString()
        };
        
        await db.collection('sales').doc(newSaleId).set(cleanFirestoreData(saleRecord));
        return { success: true, data: saleRecord.invoiceId }; 
    } catch (e: any) { 
        return { success: false, message: e.message }; 
    }
};

/**
 * updateSaleStatus (FIXED):
 * 1. Implements re-stocking logic when an order is cancelled.
 * 2. Fetches the sale record to find associated items and restores their quantity to stock.
 */
export const updateSaleStatus = async (saleId: string, status: SalesStatus, additionalData?: any): Promise<ApiResponse<SalesRecord>> => {
  try {
    // Restore inventory if order is cancelled
    if (status === 'CANCELLED') {
      const saleRef = db.collection('sales').doc(saleId);
      const saleSnap = await saleRef.get();
      
      if (saleSnap.exists) {
        const saleData = saleSnap.data() as SalesRecord;
        
        // Prevent double restocking if already cancelled
        if (saleData.status !== 'CANCELLED') {
          const restockPromises = (saleData.items || []).map(item => {
            return db.collection('finished_goods').doc(item.finishedGoodId).update({
              quantity: firebase.firestore.FieldValue.increment(item.quantity)
            });
          });
          await Promise.all(restockPromises);
        }
      }
    }

    await db.collection('sales').doc(saleId).update(cleanFirestoreData({ status, ...additionalData }));
    return { success: true };
  } catch (error: any) {
    return { success: false, message: error.message };
  }
};

// --- PRODUCTION & COSTS ---

export const getDailyProductionCosts = async (forceRemote = false): Promise<ApiResponse<DailyCostMetrics[]>> => {
  try {
    const snap = await db.collection('daily_costs').orderBy('date', 'desc').get();
    const data = snap.docs.map(d => d.data() as DailyCostMetrics);
    return { success: true, data };
  } catch (e) { return { success: false, message: "Failed to fetch costs" }; }
};

export const updateDailyCost = async (id: string, updates: Partial<DailyCostMetrics>): Promise<ApiResponse<boolean>> => {
    await db.collection('daily_costs').doc(id).update(cleanFirestoreData(updates));
    return { success: true };
};

export const getWeeklyRevenue = async (): Promise<{date: string, amount: number}[]> => {
    const salesRes = await getSales();
    const revenueMap: Record<string, number> = {};
    const today = new Date();
    for(let i=6; i>=0; i--) { const d = new Date(today); d.setDate(today.getDate() - i); revenueMap[d.toISOString().split('T')[0]] = 0; }
    (salesRes.data || []).forEach(s => { 
        const date = s.dateCreated.split('T')[0]; 
        if (revenueMap[date] !== undefined && s.status === 'PAID') revenueMap[date] += s.totalAmount; 
    });
    return Object.keys(revenueMap).map(date => ({ date, amount: revenueMap[date] }));
};

// --- INVENTORY & BATCHES ---

export const fetchBatches = async (): Promise<ApiResponse<MushroomBatch[]>> => {
  const snap = await db.collection('batches').orderBy('dateReceived', 'desc').get();
  return { success: true, data: snap.docs.map(d => d.data() as MushroomBatch) };
};

/**
 * updateBatchStatus:
 * Automatically archives batches reaching 0kg into PACKED terminal state.
 * Task 2: Log processing wastage as a financial cost.
 */
export const updateBatchStatus = async (id: string, status: BatchStatus, updates: any = {}): Promise<ApiResponse<MushroomBatch>> => {
  let finalStatus = status;
  let finalUpdates = { ...updates };

  if (updates.remainingWeightKg !== undefined && updates.remainingWeightKg <= 0.001) {
    finalStatus = BatchStatus.PACKED;
    finalUpdates.remainingWeightKg = 0;
  }

  await db.collection('batches').doc(id).update(cleanFirestoreData({ status: finalStatus, ...finalUpdates }));

  // Task 2: Log Wastage at Processing
  if (updates.processingWastageKg && updates.processingWastageKg > 0) {
      const rawRate = getRawMaterialRate();
      const wastageCost = updates.processingWastageKg * rawRate;
      const costEntry: DailyCostMetrics = {
          id: `COST-WASTE-${id}-${Date.now()}`,
          referenceId: `WASTE-${id}-${Date.now()}`,
          date: new Date().toISOString(),
          weightProcessed: 0,
          processingHours: 0,
          rawMaterialCost: 0,
          packagingCost: 0,
          wastageCost: wastageCost,
          laborCost: 0,
          totalCost: wastageCost
      };
      await db.collection('daily_costs').doc(costEntry.id!).set(cleanFirestoreData(costEntry));
  }

  return { success: true };
};

export const getInventory = async (): Promise<ApiResponse<InventoryItem[]>> => {
  const snap = await db.collection('inventory').get();
  return { success: true, data: snap.docs.map(d => d.data() as InventoryItem) };
};

export const addInventoryItem = async (item: InventoryItem) => {
  await db.collection('inventory').doc(item.id).set(cleanFirestoreData(item));
  return { success: true };
};

export const deleteInventoryItem = async (id: string) => {
  await db.collection('inventory').doc(id).delete();
  return { success: true };
};

// --- REMAINING UTILS ---

export const getFinishedGoods = async (forceRemote = false) => {
  const snap = await db.collection('finished_goods').orderBy('datePacked', 'desc').get();
  return { success: true, data: snap.docs.map(d => d.data() as FinishedGood) };
};

export const updateFinishedGoodImage = async (recipeName: string, packagingType: string, file: File): Promise<ApiResponse<void>> => {
    try {
        const storageRef = storage.ref(`products/${Date.now()}_${file.name}`);
        await storageRef.put(file);
        const imageUrl = await storageRef.getDownloadURL();
        const snap = await db.collection('finished_goods').where('recipeName', '==', recipeName).where('packagingType', '==', packagingType).get();
        await Promise.all(snap.docs.map(d => d.ref.update({ imageUrl })));
        return { success: true };
    } catch (error: any) {
        return { success: false, message: error.message || 'Failed to update image' };
    }
};

export const updateFinishedGoodPrice = async (recipeName: string, packagingType: string, sellingPrice: number) => {
    const snap = await db.collection('finished_goods').where('recipeName', '==', recipeName).where('packagingType', '==', packagingType).get();
    await Promise.all(snap.docs.map(d => d.ref.update({ sellingPrice })));
    return { success: true };
};

export const getRecipes = async () => {
    const snap = await db.collection('recipes').get();
    return snap.docs.map(d => ({ id: d.id, ...d.data() } as Recipe));
};

export const saveRecipe = async (recipe: Recipe, file?: File) => {
    await db.collection('recipes').doc(recipe.id).set(cleanFirestoreData(recipe));
    return { success: true };
};

export const deleteRecipe = async (id: string) => {
    await db.collection('recipes').doc(id).delete();
    return true;
};

export const getSuppliers = async () => {
  const snap = await db.collection('suppliers').get();
  return { success: true, data: snap.docs.map(d => d.data() as Supplier) };
};

export const addSupplier = async (s: Supplier) => {
    await db.collection('suppliers').doc(s.id).set(cleanFirestoreData(s));
    return { success: true };
};

export const getPurchaseOrders = async () => {
  const snap = await db.collection('purchase_orders').orderBy('dateOrdered', 'desc').get();
  return { success: true, data: snap.docs.map(d => d.data() as PurchaseOrder) };
};

export const createPurchaseOrder = async (itemId: string, qty: number, supplier: string) => {
    const invRes = await getInventory();
    const item = (invRes.data || []).find(i => i.id === itemId);
    const unitCost = item?.unitCost || 0;
    const packSize = item?.packSize || 1;
    const totalUnits = qty * packSize;
    const totalCost = qty * unitCost;

    const po: PurchaseOrder = { 
        id: `PO-${Date.now()}`, 
        itemId, 
        itemName: item?.name || 'Item', 
        quantity: qty, 
        packSize, 
        totalUnits, 
        unitCost, 
        totalCost, 
        status: 'ORDERED', 
        dateOrdered: new Date().toISOString(), 
        supplier 
    };
    await db.collection('purchase_orders').doc(po.id).set(cleanFirestoreData(po));
    return { success: true };
};

export const receivePurchaseOrder = async (id: string, qc: boolean) => {
    try {
        const poRef = db.collection('purchase_orders').doc(id);
        const poSnap = await poRef.get();
        if (!poSnap.exists) return { success: false, message: 'Order not found' };
        const po = poSnap.data() as PurchaseOrder;
        if (qc) {
            const invRef = db.collection('inventory').doc(po.itemId);
            await invRef.update({ quantity: firebase.firestore.FieldValue.increment(po.totalUnits) });
            await poRef.update({ status: 'RECEIVED', dateReceived: new Date().toISOString(), qcPassed: true });
        } else {
            await poRef.update({ status: 'COMPLAINT', qcPassed: false });
        }
        return { success: true };
    } catch (e: any) { return { success: false, message: e.message }; }
};

export const complaintPurchaseOrder = async (id: string, reason: string) => {
    await db.collection('purchase_orders').doc(id).update({ status: 'COMPLAINT', complaintReason: reason });
    return { success: true };
};

/**
 * resolveComplaint (FIXED):
 * 1. Mark status as RESOLVED.
 * 2. If the resolution mentions 'Replacement', automatically increment inventory stock for that item.
 */
export const resolveComplaint = async (id: string, res: string): Promise<ApiResponse<void>> => {
    try {
        const poRef = db.collection('purchase_orders').doc(id);
        const poSnap = await poRef.get();
        if (!poSnap.exists) return { success: false, message: 'Order record not found' };
        
        const po = poSnap.data() as PurchaseOrder;

        // Inventory update logic for replacements
        if (res.toLowerCase().includes('replacement')) {
            const invRef = db.collection('inventory').doc(po.itemId);
            await invRef.update({
                quantity: firebase.firestore.FieldValue.increment(po.totalUnits)
            });
        }

        // Finalize PO update
        await poRef.update({ 
            status: 'RESOLVED', 
            complaintResolution: res 
        });

        return { success: true };
    } catch (error: any) {
        return { success: false, message: error.message };
    }
};

export const getMonthlyBudget = async (m: string) => {
    const snap = await db.collection('budgets').doc(m).get();
    if (snap.exists) return { success: true, data: snap.data() as Budget };
    return { success: false };
};

export const setMonthlyBudget = async (b: Budget) => {
    await db.collection('budgets').doc(b.month).set(cleanFirestoreData(b));
    return { success: true };
};

// --- LEGACY SYNC ---
export const pushFullDatabase = async () => ({ success: true, message: "Data is live on Firestore." });
export const pullFullDatabase = async () => ({ success: true, message: "Data pulled from Firestore." });

// --- THEME ---
export const THEMES = {
  mushroom: { id: 'mushroom', label: 'Mushroom Earth', colors: { '--earth-800': '#292524', '--earth-900': '#1c1917', '--earth-700': '#44403c', '--earth-500': '#78716c', '--earth-200': '#e7e5e4', '--earth-100': '#f5f5f4', '--nature-600': '#16a34a', '--nature-500': '#22c55e', '--nature-400': '#4ade80', '--nature-900': '#14532d', '--nature-50': '#f0fdf4', '--nature-200': '#bbf7d0', '--nature-700': '#15803d' } },
  midnight: { id: 'midnight', label: 'Midnight Forest', colors: { '--earth-800': '#0f172a', '--earth-900': '#020617', '--earth-700': '#1e293b', '--earth-500': '#64748b', '--earth-200': '#e2e8f0', '--earth-100': '#f1f5f9', '--nature-600': '#0891b2', '--nature-500': '#06b6d4', '--nature-400': '#22d3ee', '--nature-900': '#164e63', '--nature-50': '#ecfeff', '--nature-200': '#a5f3fc', '--nature-700': '#0e7490' } },
  // Fixed error in line 531: duplicated property '--nature-50'. First occurrence changed to '--nature-500'.
  royal: { id: 'royal', label: 'Royal Harvest', colors: { '--earth-800': '#4c1d95', '--earth-900': '#2e1065', '--earth-700': '#5b21b6', '--earth-500': '#8b5cf6', '--earth-200': '#ddd6fe', '--earth-100': '#f5f3ff', '--nature-600': '#e11d48', '--nature-500': '#f43f5e', '--nature-400': '#fb7185', '--nature-900': '#881337', '--nature-50': '#fff1f2', '--nature-200': '#fecdd3', '--nature-700': '#be123c' } }
};

export const getTheme = (): string => localStorage.getItem(STORAGE_KEY_THEME) || 'mushroom';
export const applyTheme = (themeId: string) => {
  const theme = THEMES[themeId as keyof typeof THEMES] || THEMES.mushroom;
  const root = document.documentElement;
  Object.entries(theme.colors).forEach(([variable, value]) => { root.style.setProperty(variable, value); });
  localStorage.setItem(STORAGE_KEY_THEME, themeId);
};

/**
 * createBatch:
 * Logs incoming mushroom batches.
 * Task 1: Log initial spoilage at receipt stage as a financial cost.
 */
export const createBatch = async (farm: string, raw: number, spoiled: number, farmBatchId?: string, species?: string, flush?: string) => {
    const batch: MushroomBatch = { id: `B-${Date.now()}`, dateReceived: new Date().toISOString(), sourceFarm: farm, rawWeightKg: raw, spoiledWeightKg: spoiled, netWeightKg: raw - spoiled, status: BatchStatus.RECEIVED, species, farmBatchId, flushNumber: flush };
    await db.collection('batches').doc(batch.id).set(cleanFirestoreData(batch));

    // Task 1: Log Spoilage at Receipt
    if (spoiled > 0) {
        const rawRate = getRawMaterialRate();
        const spoiledCost = spoiled * rawRate;
        const costEntry: DailyCostMetrics = {
            id: `COST-REC-${Date.now()}`,
            referenceId: batch.id,
            date: new Date().toISOString(),
            weightProcessed: 0,
            processingHours: 0,
            rawMaterialCost: 0,
            packagingCost: 0,
            wastageCost: spoiledCost,
            laborCost: 0,
            totalCost: spoiledCost
        };
        await db.collection('daily_costs').doc(costEntry.id!).set(cleanFirestoreData(costEntry));
    }

    return { success: true, data: batch };
};

/**
 * packRecipeFIFO (MODIFIED):
 * 1. Validates inventory levels to prevent negative stock.
 * 2. Correctly deducts raw material weight using FIFO logic across batches.
 * 3. Handles "residual weight" by calculating actual available weight (Remaining - Wastage).
 * 4. Archives (disappears) empty batches or those where all "good" weight is packed.
 * 5. Logs COGS (Raw Materials + Packaging + Labor) at the point of packing.
 */
export const packRecipeFIFO = async (recipeName: string, totalWeightToPack: number, packCount: number, packagingType: 'TIN' | 'POUCH'): Promise<ApiResponse<void>> => {
    try {
        const batchRes = await fetchBatches();
        const invRes = await getInventory();
        const inventory = invRes.data || [];
        const rawRate = getRawMaterialRate();
        const laborRate = getLaborRate();

        // 1. Identify & Validate Packaging & Labels in Inventory
        const pkgItem = inventory.find(i => i.subtype === packagingType || (i.name && i.name.toUpperCase().includes(packagingType.toUpperCase())));
        const labelItem = inventory.find(i => i.type === 'LABEL' || i.subtype === 'STICKER' || (i.name && (i.name.toUpperCase().includes('LABEL') || i.name.toUpperCase().includes('STICKER'))));

        if (!pkgItem) return { success: false, message: `Missing ${packagingType} item in inventory.` };
        if (pkgItem.quantity < packCount) return { success: false, message: `Insufficient ${packagingType} inventory. Have: ${pkgItem.quantity}, Need: ${packCount}` };
        
        if (!labelItem) return { success: false, message: 'Missing Labels/Stickers in inventory.' };
        if (labelItem.quantity < packCount) return { success: false, message: `Insufficient Labels inventory. Have: ${labelItem.quantity}, Need: ${packCount}` };

        // 2. FIFO Batch Weight Deduction
        let remainingToDeduct = totalWeightToPack;
        let totalProcessingHours = 0;
        const relevantBatches = (batchRes.data || [])
            .filter(b => (b.selectedRecipeName === recipeName || b.recipeType === recipeName) && 
                         (b.status === BatchStatus.DRYING_COMPLETE || b.status === BatchStatus.PACKED))
            .sort((a, b) => new Date(a.dateReceived).getTime() - new Date(b.dateReceived).getTime());

        // Validate Total Available Batch Weight (Actual Available = Gross - Wastage)
        const totalAvailWeight = relevantBatches.reduce((sum, b) => {
            const gross = b.remainingWeightKg ?? b.netWeightKg;
            const waste = b.processingWastageKg || 0;
            return sum + Math.max(0, gross - waste);
        }, 0);

        if (totalAvailWeight < totalWeightToPack) {
            return { success: false, message: `Insufficient batch weight. Have: ${totalAvailWeight.toFixed(2)}kg, Need: ${totalWeightToPack.toFixed(2)}kg` };
        }

        const batchUpdates: Promise<any>[] = [];
        for (const batch of relevantBatches) {
            if (remainingToDeduct <= 0) break;
            
            const grossRemaining = batch.remainingWeightKg ?? batch.netWeightKg;
            const wastage = batch.processingWastageKg || 0;
            const actualAvailable = Math.max(0, grossRemaining - wastage);
            
            if (actualAvailable <= 0.001) continue; // Skip batches that are essentially empty or pure waste

            let deductionFromGross = 0;
            let newStatus = batch.status;

            // FIX: If we pack everything that is available, we close the batch entirely
            if (remainingToDeduct >= actualAvailable) {
                deductionFromGross = grossRemaining; // Zero out the whole remaining gross
                remainingToDeduct -= actualAvailable;
                newStatus = BatchStatus.PACKED;
            } else {
                // Partial deduction: only deduct what we need from the gross pool
                deductionFromGross = remainingToDeduct;
                remainingToDeduct = 0;
            }

            const newRemaining = Math.max(0, grossRemaining - deductionFromGross);
            if (newRemaining <= 0.001) newStatus = BatchStatus.PACKED;

            // Track labor hours (Default to 2 hours if no config exists)
            const hours = batch.processConfig 
                ? (batch.processConfig.totalDurationSeconds / 3600) 
                : 2;
            totalProcessingHours += hours;

            batchUpdates.push(db.collection('batches').doc(batch.id).update({
                remainingWeightKg: newRemaining,
                status: newStatus,
                packedDate: new Date().toISOString()
            }));
        }

        // 3. Finalize Costs (Packing Event = Expense Recognition)
        const pkgUnitCost = (pkgItem.unitCost || 0) / (pkgItem.packSize || 1);
        const labelUnitCost = (labelItem.unitCost || 0) / (labelItem.packSize || 1);
        const totalPackingCost = packCount * (pkgUnitCost + labelUnitCost);
        const totalRawMaterialUsedCost = totalWeightToPack * rawRate;
        const totalLaborCost = totalProcessingHours * laborRate;

        const costEntry: DailyCostMetrics = {
            id: `COST-PACK-${Date.now()}`,
            referenceId: `PK-${recipeName}-${Date.now()}`,
            date: new Date().toISOString(),
            weightProcessed: totalWeightToPack,
            processingHours: totalProcessingHours,
            rawMaterialCost: totalRawMaterialUsedCost,
            packagingCost: totalPackingCost,
            wastageCost: 0,
            laborCost: totalLaborCost,
            totalCost: totalRawMaterialUsedCost + totalPackingCost + totalLaborCost
        };

        // 4. Batch Operations: Execute Firestore Updates
        const invUpdates = [
            db.collection('inventory').doc(pkgItem.id).update({ quantity: firebase.firestore.FieldValue.increment(-packCount) }),
            db.collection('inventory').doc(labelItem.id).update({ quantity: firebase.firestore.FieldValue.increment(-packCount) })
        ];

        const fg: FinishedGood = { 
            id: `FG-${Date.now()}`, batchId: 'MULTI_FIFO', recipeName, packagingType, quantity: packCount, datePacked: new Date().toISOString(), sellingPrice: 15.00 
        };

        await Promise.all([
            ...batchUpdates,
            ...invUpdates,
            db.collection('finished_goods').doc(fg.id).set(cleanFirestoreData(fg)),
            db.collection('daily_costs').doc(costEntry.id!).set(cleanFirestoreData(costEntry))
        ]);

        return { success: true };
    } catch (error: any) {
        return { success: false, message: error.message };
    }
};

export const getPackingHistory = async () => {
    const snap = await db.collection('finished_goods').limit(10).orderBy('datePacked', 'desc').get();
    return { success: true, data: snap.docs.map(d => d.data() as FinishedGood) };
};