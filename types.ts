export enum UserRole {
  PROCESSING_WORKER = 'Processing Worker',
  PACKING_STAFF = 'Packing Staff',
  FINANCE_CLERK = 'Finance Clerk',
  ADMIN = 'Admin'
}

export enum BatchStatus {
  RECEIVED = 'Received',
  PROCESSING = 'Processing',
  DRYING_COMPLETE = 'Drying Complete',
  PACKED = 'Packed',
  STORED = 'Stored',
  SOLD = 'Sold'
}

export enum RecipeType {
  CHIPS = 'Crispy Mushroom Chips',
  DRIED = 'Dried Whole Mushrooms'
}

export interface Recipe {
  id: string;
  name: string;
  type: 'CHIPS' | 'DRIED' | 'POWDER' | 'OTHER';
  baseWeightKg: number;
  cookTimeMinutes: number;
  temperature?: number;
  notes?: string;
  imageUrl?: string;
  yieldRatio?: number; 
  defaultPackSizeKg?: number; 
  ingredients?: string; 
}

export interface ProcessConfig {
  startTime: number;
  washDurationSeconds: number;
  drainDurationSeconds: number;
  cookDurationSeconds: number;
  totalDurationSeconds: number;
}

export interface MushroomBatch {
  id: string;
  farmBatchId?: string; // ID assigned by the farm
  species?: string;     // e.g. Grey Oyster
  flushNumber?: string; // e.g. 1st Flush
  dateReceived: string;
  sourceFarm: string;
  rawWeightKg: number;
  spoiledWeightKg: number;
  netWeightKg: number;
  remainingWeightKg?: number;
  status: BatchStatus;
  processConfig?: ProcessConfig; 
  qualityNotes?: string;
  qualityCheckPassed?: boolean;
  selectedRecipeId?: string; 
  selectedRecipeName?: string; 
  recipeType?: string; 
  packedDate?: string;
  qrCodeValue?: string;
  finalPackagedWeightKg?: number;
  storageLocation?: string;
  packCount?: number;
  packagingType?: 'TIN' | 'POUCH'; 
  processingWastageKg?: number; 
  wastageReason?: string; 
}

export interface FinishedGood {
  id: string;
  batchId: string;
  recipeName: string;
  packagingType: 'TIN' | 'POUCH';
  quantity: number;
  datePacked: string;
  imageUrl?: string;
  sellingPrice?: number; 
}

export interface InventoryItem {
  id: string;
  name: string;
  type: 'PACKAGING' | 'LABEL' | 'OTHER' | 'FINISHED_GOOD';
  subtype?: 'TIN' | 'POUCH' | 'STICKER'; 
  quantity: number;
  threshold: number;
  unit: string;
  unitCost: number; 
  supplier?: string;
  packSize?: number; 
}

export interface Supplier {
  id: string;
  name: string;
  contact?: string;
  address?: string;
  itemsSupplied?: string[]; 
}

export interface Complaint {
    id: string;
    poId: string;
    supplier: string;
    item: string;
    reason: string;
    status: 'OPEN' | 'RESOLVED';
    dateLogged: string;
}

export interface PurchaseOrder {
  id: string;
  itemId: string;
  itemName: string;
  quantity: number; 
  packSize: number; 
  totalUnits: number; 
  unitCost: number;
  totalCost: number;
  status: 'ORDERED' | 'RECEIVED' | 'COMPLAINT' | 'RESOLVED';
  dateOrdered: string;
  dateReceived?: string;
  supplier: string;
  notes?: string; 
  qcPassed?: boolean;
  complaintReason?: string;
  complaintResolution?: string;
}

export interface Customer {
  id: string;
  name: string;
  contact?: string;
  email?: string;
  address?: string;
  type?: 'B2B' | 'B2C'; 
  status?: 'ACTIVE' | 'INACTIVE' | 'VIP';
  notes?: string;
  joinDate?: string;
}

export type SalesStatus = 'QUOTATION' | 'INVOICED' | 'SHIPPED' | 'PAID' | 'DELIVERED' | 'CANCELLED';
export type PaymentMethod = 'CASH' | 'COD' | 'CREDIT_CARD';

export interface SalesRecord {
  id: string;
  invoiceId: string;
  customerId: string;
  customerName: string;
  customerEmail?: string;
  customerPhone?: string;
  shippingAddress?: string;
  items: {
    finishedGoodId: string;
    recipeName: string;
    packagingType: string;
    quantity: number;
    unitPrice: number;
  }[];
  totalAmount: number;
  paymentMethod: PaymentMethod;
  status: SalesStatus;
  dateCreated: string;
  dateDelivered?: string;
  cancellationReason?: string;
}

export interface DailyCostMetrics {
  id?: string;
  referenceId?: string;
  date: string;
  weightProcessed: number;
  processingHours: number; 
  rawMaterialCost: number;
  packagingCost: number;
  wastageCost: number; 
  laborCost: number; 
  totalCost: number;
}

export interface Budget {
  id: string;
  month: string; 
  targetRevenue: number;
  targetProfit: number;
  maxWastageKg?: number;
}

export interface ApiResponse<T> {
  success: boolean;
  data?: T;
  message?: string;
}