export interface Message {
  id: string;
  content: string;
  role: 'user' | 'assistant';
  timestamp: Date;
  isProcessing?: boolean;
}

export interface Product {
  value: string;
  label: string;
  id: string;
  parts: {
    name_and_contents: string;
    manufacturer_and_barcode: string;
    pack_size: string;
    small_image: string;
    price_range?: string[];
    chainnames: string;
  };
}

export interface ShoppingQuery {
  products: string[];
  location: string;
  queryType: 'basket' | 'single_product' | 'best_price';
}