import express from "express";
import cors from "cors";
import Anthropic from "@anthropic-ai/sdk";
import dotenv from "dotenv";
import { randomUUID } from "crypto";

dotenv.config();

// In-memory session storage
const sessions = new Map();

// Tool execution function - Updated to use Salim API
async function executeShoppingTool(toolName, args) {
  console.log("🚀 executeShoppingTool called with:", toolName, args);

  switch (toolName) {
    case "search_product":
      console.log("📦 Calling searchProduct with:", args.product_name);
      return await searchProduct(args.product_name);
    case "compare_results":
      console.log(
        "💰 Calling compareResults with:",
        args.product_id,
        args.shopping_address
      );
      return await compareResults(args.product_id, args.shopping_address);
    case "find_best_basket":
      console.log(
        "🛒 Calling findBestBasket with:",
        args.products,
        args.shopping_address
      );
      return await findBestBasket(args.products, args.shopping_address);
    case "get_lowest_prices":
      console.log("💸 Calling getLowestPrices with:", args.category, args.limit);
      return await getLowestPrices(args.category, args.limit);
    case "get_price_history":
      console.log("📈 Calling getPriceHistory with:", args.barcode, args.days);
      return await getPriceHistory(args.barcode, args.days);
    default:
      console.error("❌ Unknown tool:", toolName);
      throw new Error(`Unknown tool: ${toolName}`);
  }
}

// Helper function to try a search with a specific term
async function trySearch(fetch, apiUrl, searchTerm) {
  try {
    const encodedTerm = encodeURIComponent(searchTerm);
    const searchUrl = `${apiUrl}/products?q=${encodedTerm}&limit=10`;
    
    const response = await fetch(searchUrl);
    if (!response.ok) {
      console.log(`Search failed for "${searchTerm}": ${response.status}`);
      return null;
    }
    
    const results = await response.json();
    return results && results.length > 0 ? results : null;
  } catch (error) {
    console.log(`Search error for "${searchTerm}":`, error.message);
    return null;
  }
}

// Hebrew to English product translation map
const HEBREW_TO_ENGLISH_PRODUCTS = {
  'חלב': ['milk', 'dairy'],
  'לחם': ['bread'],
  'ביצים': ['eggs', 'egg'],
  'גבינה': ['cheese'],
  'יוגורט': ['yogurt'],
  'חמאה': ['butter'],
  'שמן': ['oil'],
  'סוכר': ['sugar'],
  'קמח': ['flour'],
  'אורז': ['rice'],
  'פסטה': ['pasta'],
  'עוף': ['chicken'],
  'בקר': ['beef'],
  'דג': ['fish'],
  'עגבניות': ['tomato', 'tomatoes'],
  'מלפפון': ['cucumber'],
  'בצל': ['onion'],
  'תפוח': ['apple'],
  'בננה': ['banana'],
  'תפוז': ['orange'],
  'מים': ['water'],
  'קוקה קולה': ['coca cola', 'coke'],
  'שוקולד': ['chocolate'],
  'חטיף': ['snack', 'bar'],
  'ביסקוויט': ['biscuit', 'cookie']
};

// Updated to use Salim API with Hebrew-English translation support
async function searchProduct(productName) {
  if (!productName || productName.trim() === "") {
    throw new Error("Product name is required for search");
  }
  
  console.log('🔍 Searching Salim API for product:', productName);
  
  const fetch = (await import("node-fetch")).default;
  const SALIM_API_URL = process.env.SALIM_API_URL || 'http://localhost:8000';
  
  // Try original search term first
  let salimResults = await trySearch(fetch, SALIM_API_URL, productName.trim());
  
  // If no results and the search term is Hebrew, try English translations
  if ((!salimResults || salimResults.length === 0)) {
    const searchTerm = productName.trim().toLowerCase();
    
    // Check if we have English translations for this Hebrew term
    for (const [hebrewTerm, englishTerms] of Object.entries(HEBREW_TO_ENGLISH_PRODUCTS)) {
      if (searchTerm.includes(hebrewTerm)) {
        console.log(`🌐 Trying English translations for Hebrew term: ${hebrewTerm}`);
        
        for (const englishTerm of englishTerms) {
          console.log(`🔍 Trying English search: ${englishTerm}`);
          salimResults = await trySearch(fetch, SALIM_API_URL, englishTerm);
          if (salimResults && salimResults.length > 0) {
            console.log(`✅ Found results with English term: ${englishTerm}`);
            break;
          }
        }
        
        if (salimResults && salimResults.length > 0) {
          break;
        }
      }
    }
  }
  
  console.log('📦 Found', salimResults?.length || 0, 'products in Salim database');
  
  if (!salimResults || salimResults.length === 0) {
    return {
      products: [],
      message: `לא נמצאו מוצרים עבור "${productName}". נסה חיפוש אחר או בדוק איות.`,
      searchTerm: productName
    };
  }
  
  // Transform Salim API results  
  const transformedResults = salimResults.map(product => ({
    id: product.barcode,
    product_id: product.product_id, // Database row ID
    barcode: product.barcode,
    name: product.canonical_name,
    brand: product.brand,
    category: product.category,
    price: `₪${product.price}`,
    promo_price: product.promo_price ? `₪${product.promo_price}` : null,
    promo_text: product.promo_text,
    supermarket_id: product.supermarket_id,
    size: `${product.size_value || ''} ${product.size_unit || ''}`.trim(),
    in_stock: product.in_stock,
    label: `${product.canonical_name} ${product.brand ? '- ' + product.brand : ''}`
  }));

  return {
    products: transformedResults,
    message: `נמצאו ${transformedResults.length} מוצרים עבור "${productName}"`,
    searchTerm: productName,
    bestMatch: transformedResults[0]
  };
}

// Updated to use Salim API
async function compareResults(productId, shoppingAddress) {
  if (!productId || productId.toString().trim() === "") {
    throw new Error("Product ID or barcode is required for price comparison");
  }

  console.log('💰 Comparing prices in Salim API for:', productId, 'near:', shoppingAddress);

  const fetch = (await import("node-fetch")).default;
  
  let comparisonData;
  
  try {
    // Try using it as a barcode first
    const SALIM_API_URL = process.env.SALIM_API_URL || 'http://localhost:8000';
    const barcodeUrl = `${SALIM_API_URL}/products/barcode/${encodeURIComponent(productId.toString().trim())}`;
    const response = await fetch(barcodeUrl);
    
    if (response.ok) {
      comparisonData = await response.json();
      console.log('📊 Found price comparison by barcode across', comparisonData?.length || 0, 'supermarkets');
    } else {
      // Try as product ID
      const productResponse = await fetch(`${SALIM_API_URL}/products/${productId}`);
      if (productResponse.ok) {
        const product = await productResponse.json();
        const barcodeResponse = await fetch(`${SALIM_API_URL}/products/barcode/${product.barcode}`);
        if (barcodeResponse.ok) {
          comparisonData = await barcodeResponse.json();
          console.log('📊 Found price comparison by product ID->barcode');
        } else {
          throw new Error('No price comparison data available');
        }
      } else {
        throw new Error('Product not found');
      }
    }
  } catch (error) {
    throw new Error(`Price comparison failed: ${error.message}`);
  }

  if (!comparisonData || comparisonData.length === 0) {
    return {
      success: false,
      message: `לא נמצאו נתוני השווה מחירים עבור מוצר ${productId}`,
      productId: productId,
      location: shoppingAddress
    };
  }

  const comparison = {
    product_name: comparisonData[0]?.canonical_name || 'Unknown Product',
    brand: comparisonData[0]?.brand || '',
    category: comparisonData[0]?.category || '',
    barcode: comparisonData[0]?.barcode || '',
    size_info: `${comparisonData[0]?.size_value || ''} ${comparisonData[0]?.size_unit || ''}`.trim(),
    shopping_location: shoppingAddress,
    stores: comparisonData.map(item => ({
      supermarket: item.supermarket_name,
      price: parseFloat(item.price),
      promo_price: item.promo_price ? parseFloat(item.promo_price) : null,
      effective_price: item.promo_price ? parseFloat(item.promo_price) : parseFloat(item.price),
      promo_text: item.promo_text,
      savings: item.savings ? parseFloat(item.savings) : null,
      in_stock: item.in_stock,
      currency: 'ILS'
    })),
    best_price: Math.min(...comparisonData.map(item => item.promo_price ? parseFloat(item.promo_price) : parseFloat(item.price))),
    cheapest_store: comparisonData[0]?.supermarket_name,
    total_stores: comparisonData.length,
    success: true
  };

  return comparison;
}

// Updated to use Salim API
async function findBestBasket(products, shoppingAddress) {
  if (!shoppingAddress) {
    throw new Error("Shopping address is required for basket comparison");
  }

  if (!products || products.length === 0) {
    throw new Error("Products are required for basket comparison");
  }

  console.log('🏪 Finding best basket using Salim API for:', products, 'near:', shoppingAddress);

  try {
    const fetch = (await import("node-fetch")).default;
    
    // Step 1: Search for each product
    const productSearchResults = [];
    const searchErrors = [];

    for (const productName of products) {
      try {
        const searchResult = await searchProduct(productName);
        if (searchResult.success !== false && searchResult.products && searchResult.products.length > 0) {
          productSearchResults.push({
            productName,
            product: searchResult.products[0] // Take the best match
          });
        } else {
          searchErrors.push(`No results for: ${productName}`);
        }
      } catch (error) {
        searchErrors.push(`Search failed for ${productName}: ${error.message}`);
      }
    }

    if (productSearchResults.length === 0) {
      return {
        success: false,
        message: `לא נמצאו מוצרים עבור הרשימה שלך. שגיאות: ${searchErrors.join(', ')}`,
        searchErrors
      };
    }

    // Step 2: Get price comparisons and build baskets
    const basketData = {};
    const supermarketNames = {
      1: 'Rami Levi',
      2: 'Yohananof', 
      3: 'Carrefour'
    };

    // Initialize baskets
    for (const [id, name] of Object.entries(supermarketNames)) {
      basketData[name] = {
        supermarket_id: parseInt(id),
        supermarket_name: name,
        products: [],
        total_price: 0,
        total_promo_price: 0,
        total_savings: 0,
        product_count: 0,
        location: shoppingAddress
      };
    }

    // Process each product
    for (const productResult of productSearchResults) {
      try {
        const comparison = await compareResults(productResult.product.barcode, shoppingAddress);
        
        if (comparison.success && comparison.stores) {
          for (const store of comparison.stores) {
            if (basketData[store.supermarket]) {
              basketData[store.supermarket].products.push({
                name: comparison.product_name,
                brand: comparison.brand,
                price: store.price,
                promo_price: store.promo_price,
                effective_price: store.effective_price,
                savings: store.savings || 0,
                promo_text: store.promo_text,
                in_stock: store.in_stock
              });
              
              basketData[store.supermarket].total_price += store.price;
              basketData[store.supermarket].total_promo_price += store.effective_price;
              basketData[store.supermarket].total_savings += store.savings || 0;
              basketData[store.supermarket].product_count++;
            }
          }
        }
      } catch (error) {
        console.warn(`Could not get prices for ${productResult.productName}:`, error.message);
      }
    }

    // Step 3: Calculate final results
    const completeBaskets = Object.values(basketData)
      .filter(basket => basket.product_count === productSearchResults.length)
      .map(basket => ({
        ...basket,
        total_price: Math.round(basket.total_price * 100) / 100,
        total_promo_price: Math.round(basket.total_promo_price * 100) / 100,
        total_savings: Math.round(basket.total_savings * 100) / 100,
        average_price_per_product: Math.round((basket.total_promo_price / basket.product_count) * 100) / 100
      }))
      .sort((a, b) => a.total_promo_price - b.total_promo_price);

    return {
      success: true,
      baskets: completeBaskets,
      best_basket: completeBaskets[0] || null,
      shopping_location: shoppingAddress,
      summary: {
        total_products_requested: products.length,
        total_products_found: productSearchResults.length,
        stores_with_complete_baskets: completeBaskets.length,
        best_total_price: completeBaskets[0]?.total_promo_price || 0,
        worst_total_price: completeBaskets[completeBaskets.length - 1]?.total_promo_price || 0,
        max_savings: completeBaskets.length > 1 ? 
          Math.round((completeBaskets[completeBaskets.length - 1].total_promo_price - completeBaskets[0].total_promo_price) * 100) / 100 : 0,
        search_errors: searchErrors
      }
    };

  } catch (error) {
    return {
      success: false,
      message: `Failed to find best basket: ${error.message}`,
      error: error.message
    };
  }
}

// New tool: Get lowest prices in each store
async function getLowestPrices(category = null, limit = 10) {
  console.log('💸 Getting lowest prices from Salim API...');
  
  const fetch = (await import("node-fetch")).default;
  const SALIM_API_URL = process.env.SALIM_API_URL || 'http://localhost:8000';
  
  let apiUrl = `${SALIM_API_URL}/products/lowest-prices?limit=${limit}`;
  if (category) {
    apiUrl += `&category=${encodeURIComponent(category)}`;
  }
  
  try {
    const response = await fetch(apiUrl);
    if (!response.ok) {
      throw new Error(`Lowest prices API failed: ${response.status} ${response.statusText}`);
    }
    
    const lowestPrices = await response.json();
    console.log('💸 Found', lowestPrices?.length || 0, 'lowest price items');
    
    if (!lowestPrices || lowestPrices.length === 0) {
      return {
        lowest_prices: [],
        message: category ? 
          `לא נמצאו מבצעים בקטגוריה "${category}". נסה קטגוריה אחרת.` :
          "לא נמצאו מבצעים כרגע.",
        category: category
      };
    }
    
    // Transform results for Hebrew interface
    const transformedResults = lowestPrices.map(item => ({
      product_id: item.product_id || null, // Database row ID
      supermarket: item.supermarket_name,
      supermarket_id: item.supermarket_id,
      name: item.canonical_name,
      brand: item.brand,
      category: item.category,
      barcode: item.barcode,
      regular_price: `₪${item.price}`,
      promo_price: item.promo_price ? `₪${item.promo_price}` : null,
      final_price: `₪${item.effective_price}`,
      savings_percent: item.savings_percent ? `${Math.round(item.savings_percent)}%` : null,
      is_on_sale: !!item.promo_price
    }));
    
    return {
      lowest_prices: transformedResults,
      message: category ? 
        `נמצאו ${transformedResults.length} המוצרים הזולים ביותר בקטגוריה "${category}"` :
        `נמצאו ${transformedResults.length} המוצרים הזולים ביותר בכל חנות`,
      category: category,
      best_deals: transformedResults.filter(item => item.savings_percent)
    };
    
  } catch (error) {
    console.error('❌ Error getting lowest prices:', error);
    return {
      lowest_prices: [],
      message: `שגיאה בחיפוש המבצעים: ${error.message}`,
      error: error.message
    };
  }
}

// New tool: Get price history for a product
async function getPriceHistory(barcode, days = 30) {
  if (!barcode || barcode.toString().trim() === "") {
    throw new Error("Barcode is required for price history");
  }
  
  console.log(`📈 Getting price history for barcode ${barcode} over ${days} days...`);
  
  const fetch = (await import("node-fetch")).default;
  const SALIM_API_URL = process.env.SALIM_API_URL || 'http://localhost:8000';
  const apiUrl = `${SALIM_API_URL}/products/price-history/${barcode}?days=${days}`;
  
  try {
    const response = await fetch(apiUrl);
    if (!response.ok) {
      if (response.status === 404) {
        return {
          success: false,
          message: `לא נמצא היסטוריית מחירים עבור ברקוד ${barcode}. ייתכן שהמוצר חדש או לא קיים במערכת.`,
          barcode: barcode
        };
      }
      throw new Error(`Price history API failed: ${response.status} ${response.statusText}`);
    }
    
    const historyData = await response.json();
    console.log('📈 Found price history with', historyData.price_history?.length || 0, 'entries');
    
    // Transform for Hebrew interface
    const transformedHistory = historyData.price_history.map(entry => ({
      product_id: entry.product_id, // Database row ID
      date: new Date(entry.date).toLocaleDateString('he-IL'),
      supermarket: entry.supermarket_name,
      regular_price: `₪${entry.price}`,
      promo_price: entry.promo_price ? `₪${entry.promo_price}` : null,
      final_price: `₪${entry.effective_price}`,
      is_on_sale: !!entry.promo_price
    }));
    
    return {
      success: true,
      barcode: historyData.barcode,
      product_name: historyData.canonical_name,
      brand: historyData.brand,
      category: historyData.category,
      price_history: transformedHistory,
      current_lowest: `₪${historyData.current_lowest_price}`,
      current_highest: `₪${historyData.current_highest_price}`,
      price_trend: historyData.price_trend === 'increasing' ? 'עולה' :
                   historyData.price_trend === 'decreasing' ? 'יורד' : 'יציב',
      days_analyzed: days,
      message: `היסטוריית מחירים עבור ${historyData.canonical_name} ב-${days} הימים האחרונים`
    };
    
  } catch (error) {
    console.error('❌ Error getting price history:', error);
    return {
      success: false,
      message: `שגיאה בקבלת היסטוריית מחירים: ${error.message}`,
      barcode: barcode,
      error: error.message
    };
  }
}

const anthropic = new Anthropic({
  apiKey: process.env.ANTHROPIC_API_KEY,
});

// Available tools for Claude
const availableTools = [
  {
    name: "search_product",
    description: "Search for products by name in Israeli supermarkets using Salim database. Returns detailed product information including prices, brands, and availability.",
    input_schema: {
      type: "object",
      properties: {
        product_name: {
          type: "string",
          description: "The product name to search for (Hebrew/English)",
        },
      },
      required: ["product_name"],
    },
  },
  {
    name: "compare_results",
    description: "Compare prices for a specific product across different Israeli supermarkets using Salim database. Shows price differences and identifies the cheapest option.",
    input_schema: {
      type: "object",
      properties: {
        product_id: {
          type: "string",
          description: "The product ID or barcode (from search results)",
        },
        shopping_address: {
          type: "string",
          description: "Israeli city or address for context",
        },
      },
      required: ["product_id", "shopping_address"],
    },
  },
  {
    name: "find_best_basket",
    description: "Find the best shopping basket combinations across Rami Levi, Yohananof, and Carrefour supermarkets. Analyzes multiple products and finds the most cost-effective total basket.",
    input_schema: {
      type: "object",
      properties: {
        products: {
          type: "array",
          items: {
            type: "string",
          },
          description: "Array of product names to include in the basket",
        },
        shopping_address: {
          type: "string",
          description: "Israeli city or address for context",
        },
      },
      required: ["products", "shopping_address"],
    },
  },
  {
    name: "get_lowest_prices",
    description: "Find the lowest priced products in each Israeli supermarket. Shows the best deals and discounts available right now. Can filter by category to find specific types of deals.",
    input_schema: {
      type: "object",
      properties: {
        category: {
          type: "string",
          description: "Optional product category to filter by (e.g., 'Dairy', 'Snacks', 'Beverages')",
        },
        limit: {
          type: "number",
          description: "Maximum number of lowest price items to return (default: 10)",
          default: 10
        },
      },
      required: [],
    },
  },
  {
    name: "get_price_history",
    description: "Get price history and trends for a specific product by barcode. Shows how prices changed over time across different supermarkets and indicates if prices are increasing, decreasing, or stable.",
    input_schema: {
      type: "object",
      properties: {
        barcode: {
          type: "string",
          description: "The product barcode to get price history for",
        },
        days: {
          type: "number",
          description: "Number of days to look back for price history (default: 30, max: 365)",
          default: 30
        },
      },
      required: ["barcode"],
    },
  },
];

const app = express();
app.use(cors());
app.use(express.json());

// Health check endpoint
app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    timestamp: new Date().toISOString(),
    connected_to: "Salim API",
    api_url: process.env.SALIM_API_URL || "http://localhost:8000"
  });
});

// Main chat endpoint
app.post("/chat", async (req, res) => {
  try {
    const { message, sessionId } = req.body;

    if (!message || message.trim() === "") {
      return res.status(400).json({ error: "Message is required" });
    }

    // Get or create session
    let currentSessionId = sessionId || randomUUID();
    let sessionData = sessions.get(currentSessionId) || { messages: [] };

    console.log(`💬 Processing message for session ${currentSessionId}:`, message);

    // Add user message to session
    sessionData.messages.push({ role: "user", content: message });

    // Prepare system message
    const systemMessage = {
      role: "system",
      content: `אתה עוזר קניות חכם לשוק הישראלי. אתה עובד עם מאגר נתונים של Salim שמכיל מחירים בזמן אמת מרמי לוי, יוחננוף וקרפור.

כללי תפעול:
- תמיד תענה בעברית בצורה ידידותית ומועילה
- השתמש בכלים שלך לחיפוש מוצרים והשוואת מחירים
- תן המלצות קונקרטיות איפה כדאי לקנות
- הסבר על חיסכון פוטנציאלי
- אם יש מבצעים, ציין אותם
- תמיד ציין מחירים בשקלים (₪)

הכלים שלך מחוברים למאגר Salim עם נתונים אמיתיים מ:
- רמי לוי (בדרך כלל הזול ביותר)  
- יוחננוף (רשת פרמיום)
- קרפור (רשת בינלאומית)

דוגמאות לשאלות שאתה יכול לענות עליהן:
- "איפה הכי זול לקנות חלב?"
- "כמה עולה לחם בכל החנויות?"
- "איפה כדאי לי לקנות את הסל שלי?"
- "מה המחיר של ביצים ברמי לוי?"`,
    };

    const toolExecutionLogs = [];
    let totalToolsUsed = 0;
    const startTime = Date.now();

    try {
      // Call Claude with tools
      console.log("🤖 Calling Claude with tools...");
      
      const response = await anthropic.messages.create({
        model: "claude-3-5-sonnet-20241022",
        max_tokens: 2000,
        system: systemMessage.content,
        messages: sessionData.messages,
        tools: availableTools,
      });

      let finalResponse = "";
      
      // Handle tool use
      if (response.content.some(block => block.type === "tool_use")) {
        console.log("🔧 Claude wants to use tools");
        
        const toolUses = response.content.filter(block => block.type === "tool_use");
        toolExecutionLogs.push({
          type: "claude_decision",
          tools: toolUses.map(tu => ({ name: tu.name, input: tu.input }))
        });

        const toolResults = [];
        
        for (const toolUse of toolUses) {
          totalToolsUsed++;
          console.log(`🔧 Executing tool: ${toolUse.name}`);
          
          toolExecutionLogs.push({
            type: "tool_execution_start",
            toolName: toolUse.name,
            input: toolUse.input
          });

          const toolStartTime = Date.now();
          
          try {
            const result = await executeShoppingTool(toolUse.name, toolUse.input);
            const executionTime = Date.now() - toolStartTime;
            
            const resultText = typeof result === 'object' ? JSON.stringify(result, null, 2) : String(result);
            
            toolResults.push({
              tool_use_id: toolUse.id,
              type: "tool_result",
              content: resultText,
            });

            toolExecutionLogs.push({
              type: "tool_execution_success",
              toolName: toolUse.name,
              executionTime,
              resultType: typeof result,
              resultLength: resultText.length,
              resultPreview: resultText.slice(0, 100) + (resultText.length > 100 ? '...' : '')
            });

          } catch (error) {
            console.error(`❌ Tool execution error for ${toolUse.name}:`, error);
            
            toolResults.push({
              tool_use_id: toolUse.id,
              type: "tool_result",
              content: `Error: ${error.message}`,
              is_error: true,
            });

            toolExecutionLogs.push({
              type: "tool_execution_error",
              toolName: toolUse.name,
              error: error.message
            });
          }
        }

        // Get final response from Claude with tool results
        console.log("🧠 Getting final response from Claude...");
        toolExecutionLogs.push({ type: "claude_final_request" });

        const messages = [...sessionData.messages];
        messages.push({
          role: "assistant",
          content: response.content,
        });
        messages.push({
          role: "user",
          content: toolResults,
        });

        const finalResponseResult = await anthropic.messages.create({
          model: "claude-3-5-sonnet-20241022",
          max_tokens: 2000,
          system: systemMessage.content,
          messages: messages,
        });

        finalResponse = finalResponseResult.content[0]?.text || "מצטער, לא הצלחתי לעבד את הבקשה.";
        
        toolExecutionLogs.push({
          type: "claude_final_response",
          responseLength: finalResponse.length,
          responsePreview: finalResponse.slice(0, 100) + (finalResponse.length > 100 ? '...' : '')
        });

      } else {
        // Direct response without tools
        finalResponse = response.content[0]?.text || "מצטער, לא הצלחתי לעבד את הבקשה.";
        
        toolExecutionLogs.push({
          type: "claude_direct_response",
          responseLength: finalResponse.length
        });
      }

      // Add assistant response to session
      sessionData.messages.push({ role: "assistant", content: finalResponse });
      sessions.set(currentSessionId, sessionData);

      const executionTime = Date.now() - startTime;

      res.json({
        reply: finalResponse,
        sessionId: currentSessionId,
        toolExecutionLogs,
        debug: {
          totalToolsUsed,
          executionTime,
          logCount: toolExecutionLogs.length
        }
      });

    } catch (error) {
      console.error("❌ Claude API error:", error);
      res.status(500).json({
        error: "שגיאה בעיבוד הבקשה",
        details: error.message
      });
    }

  } catch (error) {
    console.error("❌ General error:", error);
    res.status(500).json({
      error: "שגיאה כללית בשרת",
      details: error.message
    });
  }
});

const PORT = process.env.PORT || 3001;

app.listen(PORT, () => {
  console.log(`🚀 Salim Shopping Chat Proxy Server running on port ${PORT}`);
  const SALIM_API_URL = process.env.SALIM_API_URL || 'http://localhost:8000';
  console.log(`🔗 Connected to Salim API at ${SALIM_API_URL}`);
  console.log(`📱 Ready to process Hebrew shopping queries!`);
});

export default app;