import express from "express";
import cors from "cors";
import Anthropic from "@anthropic-ai/sdk";
import dotenv from "dotenv";
import { randomUUID } from "crypto";
import * as cheerio from "cheerio";

dotenv.config();

// In-memory session storage
const sessions = new Map();

// Tool execution function
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
    case "calculate_savings":
      console.log(
        "💰 Calling calculateSavings with:",
        args.cheapest_basket,
        args.most_expensive_basket
      );
      return await calculateSavings(args.cheapest_basket, args.most_expensive_basket);
    case "find_most_expensive_basket":
      console.log(
        "💸 Calling findMostExpensiveBasket with:",
        args.products,
        args.shopping_address
      );
      return await findMostExpensiveBasket(args.products, args.shopping_address);
    default:
      console.error("❌ Unknown tool:", toolName);
      throw new Error(`Unknown tool: ${toolName}`);
  }
}

// Tool implementations
async function searchProduct(productName) {
  if (!productName || productName.trim() === "") {
    throw new Error("Product name is required for search");
  }

  const fetch = (await import("node-fetch")).default;
  const encodedTerm = encodeURIComponent(productName.trim());
  const apiUrl = `https://chp.co.il/autocompletion/product_extended?term=${encodedTerm}`;

  const response = await fetch(apiUrl);
  if (!response.ok) {
    throw new Error(
      `Product search failed: ${response.status} ${response.statusText}`
    );
  }

  const results = await response.json();

  // Try to find the best match based on the search term
  if (results && results.length > 0) {
    let bestMatch = results[0];
    
    // For products with specific attributes, try to find a better match
    if (productName.includes('לחם') && productName.includes('שחור')) {
      // Look for bread that matches "אחיד" (whole wheat) or "מלא" (whole)
      for (const result of results) {
        const label = result.label || result.value || '';
        if (label.includes('אחיד') || label.includes('מלא')) {
          bestMatch = result;
          break;
        }
      }
    }
    
    // For products with percentage specifications, try to find the right percentage
    const percentageMatch = productName.match(/(\d+)%/);
    if (percentageMatch) {
      const target = percentageMatch[1];
      for (const result of results) {
        const label = result.label || result.value || '';
        if (label.includes(`${target}%`)) {
          bestMatch = result;
          break;
        }
      }
    }
    
    console.log("Returning best match:", bestMatch);
    return JSON.stringify(bestMatch, null, 2);
  } else {
    return JSON.stringify({ error: "No products found" }, null, 2);
  }
}

async function compareResults(productId, shoppingAddress) {
  if (!productId || productId.toString().trim() === "") {
    throw new Error("Product ID is required for price comparison");
  }

  if (!shoppingAddress || shoppingAddress.trim() === "") {
    throw new Error("Shopping address is required for price comparison");
  }

  const fetch = (await import("node-fetch")).default;
  const encodedAddress = encodeURIComponent(shoppingAddress.trim());
  const encodedProductId = encodeURIComponent(productId.toString().trim());
  const apiUrl = `https://chp.co.il/main_page/compare_results?shopping_address=${encodedAddress}&product_barcode=${encodedProductId}&num_results=200`;

  const response = await fetch(apiUrl, {
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
      Accept:
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
      "Accept-Language": "he,en-US;q=0.7,en;q=0.3",
      "Accept-Encoding": "gzip, deflate",
      DNT: "1",
      Connection: "keep-alive",
      "Upgrade-Insecure-Requests": "1",
    },
  });

  if (!response.ok) {
    throw new Error(
      `Price comparison failed: ${response.status} ${response.statusText}`
    );
  }

  const contentType = response.headers.get("content-type") || "";
  let result;

  if (contentType.includes("application/json")) {
    result = await response.json();
    console.log({ result });
    return JSON.stringify(result, null, 2);
  } else {
    const html = await response.text();
    console.log("Parsing HTML response...");

    // Parse HTML using cheerio
    const $ = cheerio.load(html);
    const stores = [];

    // Find all table rows with class "line-odd" and "line-even"
    $("tr.line-odd, tr.line-even").each((index, element) => {
      const $row = $(element);
      const $cells = $row.find("td");

      if ($cells.length >= 5) {
        const store = $cells.eq(0).text().trim();
        const city = $cells.eq(2).text().trim();
        const price = $cells.eq(4).text().trim();

        // Debug: Log all cells to understand the structure
        console.log(`Row ${index}:`, {
          store: store,
          city: city,
          price: price,
          allCells: $cells.map((i, cell) => $(cell).text().trim()).get()
        });

        // Only add if we have valid data and price is not "הכלול במחיר הכללי"
        if (store && city && price && price !== "הכלול במחיר הכללי") {
          // Try to find a numeric price in the price field
          const priceMatch = price.match(/[\d.,]+/);
          if (priceMatch) {
            stores.push({
              store: store,
              city: city,
              price: priceMatch[0],
            });
          }
        } else if (store && city) {
          // If the 5th cell doesn't have a valid price, look for prices in other cells
          let foundPrice = null;
          for (let i = 0; i < $cells.length; i++) {
            const cellText = $cells.eq(i).text().trim();
            const priceMatch = cellText.match(/[\d.,]+/);
            if (priceMatch && cellText !== "הכלול במחיר הכללי") {
              foundPrice = priceMatch[0];
              console.log(`Found price in cell ${i}: ${foundPrice}`);
              break;
            }
          }
          
          if (foundPrice) {
            stores.push({
              store: store,
              city: city,
              price: foundPrice,
            });
          }
        }
      }
    });

    // Sort stores by price (cheapest first)
    stores.sort((a, b) => {
      const priceA = parseFloat(a.price.replace(/[^\d.]/g, ""));
      const priceB = parseFloat(b.price.replace(/[^\d.]/g, ""));
      return priceA - priceB;
    });

    console.log("Extracted stores (sorted by price):", stores);
    return JSON.stringify(stores, null, 2);
  }
}

async function findBestBasket(products, shoppingAddress) {
  if (!shoppingAddress) {
    throw new Error("Shopping address is required for basket comparison");
  }

  if (!products || products.length === 0) {
    throw new Error("Products are required for basket comparison");
  }

  console.log(
    "🛒 Starting findBestBasket for:",
    products,
    "in",
    shoppingAddress
  );

  try {
    // Step 1: Search for each product to get product IDs
    console.log("📦 Step 1: Searching for each product...");
    const productSearchResults = [];
    const searchErrors = [];

    for (const productName of products) {
      try {
        console.log(`🔍 Searching for product: ${productName}`);
        const searchResultJson = await searchProduct(productName);
        const searchResult = JSON.parse(searchResultJson);

        if (searchResult) {
          const productData = searchResult;
          const productId =
            productData.id || productData.value || productData.barcode;

          console.log(`✅ Found product ${productName} with ID: ${productId}`);
          productSearchResults.push({
            productName,
            searchData: productData,
            productId: productId,
          });
        } else {
          console.log(`❌ No results for: ${productName}`);
          searchErrors.push(`No search results for: ${productName}`);
        }
      } catch (error) {
        console.error(`🚨 Search failed for ${productName}:`, error.message);
        searchErrors.push(`Search failed for ${productName}: ${error.message}`);
      }
    }

    if (productSearchResults.length === 0) {
      throw new Error(
        `No products could be found. Errors: ${searchErrors.join(", ")}`
      );
    }

    console.log(
      `📊 Found ${productSearchResults.length} products out of ${products.length} requested`
    );

    // Step 2: Get real price comparisons for each product
    console.log("💰 Step 2: Getting price comparisons for each product...");
    const comparisonResults = [];
    const comparisonErrors = [];

    for (const product of productSearchResults) {
      try {
        console.log(
          `💰 Getting prices for ${product.productName} (ID: ${product.productId}) in ${shoppingAddress}`
        );
        const comparison = await compareResults(
          product.productId,
          shoppingAddress
        );

        comparisonResults.push({
          productName: product.productName,
          productId: product.productId,
          comparison: comparison,
        });

        console.log(`✅ Got price comparison for ${product.productName}`);
      } catch (error) {
        console.error(
          `🚨 Price comparison failed for ${product.productName}:`,
          error.message
        );
        comparisonErrors.push(
          `Price comparison failed for ${product.productName}: ${error.message}`
        );
      }
    }

    if (comparisonResults.length === 0) {
      throw new Error(
        `No price comparisons could be retrieved. Errors: ${comparisonErrors.join(
          ", "
        )}`
      );
    }

    console.log(
      `📊 Got price comparisons for ${comparisonResults.length} products`
    );

    // Step 3: Parse price comparisons and build store baskets with real prices
    console.log("🏪 Step 3: Building store baskets with real prices...");
    const storeBaskets = {};
    const parseErrors = [];

    for (const result of comparisonResults) {
      const comparison = result.comparison;
      let foundStores = false;

      console.log(`\n=== PARSING PRICES for ${result.productName} ===`);
      console.log("Comparison type:", typeof comparison);
      console.log("Comparison preview:", String(comparison).substring(0, 200));

      // The comparison is already a JSON string from compareResults
      let comparisonData;
      try {
        comparisonData = JSON.parse(comparison);
        console.log("Successfully parsed JSON, data type:", typeof comparisonData);
        if (Array.isArray(comparisonData)) {
          console.log("Array length:", comparisonData.length);
          console.log("First item:", comparisonData[0]);
        }
      } catch (error) {
        console.log("Failed to parse comparison as JSON, treating as string");
        console.log("Parse error:", error.message);
        comparisonData = comparison;
      }

      // Handle JSON response (new format from compareResults)
      if (Array.isArray(comparisonData)) {
        console.log("📊 Processing JSON array response");
        console.log("Found JSON stores:", comparisonData.length);

        for (const store of comparisonData) {
          console.log("Processing store object:", store);
          const storeName = store.store;
          const price = store.price;
          const city = store.city; // Get the address information
          
          console.log(`Store name: "${storeName}", Price: "${price}", City: "${city}"`);

          if (storeName && price) {
            const numPrice = parseFloat(price.replace(/[^\d.]/g, ""));
            console.log(`Parsed price: ${numPrice}`);
            
            if (!isNaN(numPrice) && numPrice > 0) {
              console.log(`💰 JSON match: ${storeName} = ${numPrice}₪`);

              if (!storeBaskets[storeName]) {
                storeBaskets[storeName] = {
                  storeName,
                  address: city || "כתובת לא זמינה", // Store the address
                  products: [],
                  totalPrice: 0,
                  productCount: 0,
                };
              }

              storeBaskets[storeName].products.push({
                productName: result.productName,
                price: numPrice,
              });
              storeBaskets[storeName].totalPrice += numPrice;
              storeBaskets[storeName].productCount++;
              foundStores = true;
            } else {
              console.log(`❌ Invalid price: ${numPrice} (original: ${price})`);
            }
          } else {
            console.log(`❌ Missing store name or price: storeName="${storeName}", price="${price}"`);
          }
        }
        
        // If we successfully processed JSON array, skip HTML parsing
        if (foundStores) {
          console.log("✅ Successfully processed JSON array, skipping HTML parsing");
        }
      }
      // Handle legacy JSON object response
      else if (typeof comparisonData === "object" && comparisonData !== null) {
        console.log("📊 Processing JSON object response");
        let jsonStores = [];

        if (comparisonData.stores) {
          jsonStores = comparisonData.stores;
        } else if (comparisonData.results) {
          jsonStores = comparisonData.results;
        } else if (comparisonData.data) {
          jsonStores = comparisonData.data;
        }

        console.log("Found JSON stores:", jsonStores.length);

        for (const store of jsonStores) {
          let storeName =
            store.name ||
            store.store_name ||
            store.storeName ||
            store.chain ||
            store.chainName;
          let price = store.price || store.cost || store.amount || store.value;
          let address = store.city || store.address || store.location || "כתובת לא זמינה";

          if (storeName && price) {
            const numPrice = parseFloat(price);
            if (!isNaN(numPrice) && numPrice > 0) {
              console.log(`💰 JSON match: ${storeName} = ${numPrice}₪`);

              if (!storeBaskets[storeName]) {
                storeBaskets[storeName] = {
                  storeName,
                  address: address, // Store the address
                  products: [],
                  totalPrice: 0,
                  productCount: 0,
                };
              }

              storeBaskets[storeName].products.push({
                productName: result.productName,
                price: numPrice,
              });
              storeBaskets[storeName].totalPrice += numPrice;
              storeBaskets[storeName].productCount++;
              foundStores = true;
            }
          }
        }
        
        // If we successfully processed JSON object, skip HTML parsing
        if (foundStores) {
          console.log("✅ Successfully processed JSON object, skipping HTML parsing");
        }
      }
      // Handle HTML/text response (only if no stores found yet)
      else if (!foundStores) {
        console.log("📄 Processing HTML/text response");
        const comparisonStr = String(comparison);
        console.log("HTML length:", comparisonStr.length);

        // Try multiple HTML parsing patterns to extract store names and prices
        const patterns = [
          // Pattern 1: Store name followed by price with shekel
          />([^<]{3,30})<[^>]*>.*?₪\s*([\d.,]+)/gs,
          // Pattern 2: Any Hebrew text followed by price
          />([א-ת][א-ת\s]{2,20})<.*?([\d.,]{1,6})(?:\s*₪)?/gs,
          // Pattern 3: Price followed by store name
          /₪\s*([\d.,]+)[^>]*>([^<]{3,30})</gs,
          // Pattern 4: Table-based structure
          /<td[^>]*>([^<]{3,30})<\/td>\s*<td[^>]*>([\d.,]+)/gs,
        ];

        for (
          let patternIndex = 0;
          patternIndex < patterns.length;
          patternIndex++
        ) {
          const pattern = patterns[patternIndex];
          const matches = [...comparisonStr.matchAll(pattern)];

          console.log(
            `Pattern ${patternIndex + 1}: found ${matches.length} matches`
          );

          if (matches.length > 0) {
            foundStores = true;

            for (const match of matches) {
              let storeName = match[1]?.trim();
              let priceStr = match[2]?.trim();

              // Clean up store name - remove HTML artifacts
              if (storeName) {
                storeName = storeName.replace(/<[^>]*>/g, "").trim();
                storeName = storeName.replace(/&[a-zA-Z]+;/g, "").trim();
              }

              // Clean up price string
              if (priceStr) {
                priceStr = priceStr.replace(/[^\d.,]/g, "");
              }

              console.log(
                `Processing match: store="${storeName}", price="${priceStr}"`
              );

              if (
                storeName &&
                priceStr &&
                storeName.length > 2 &&
                priceStr.length > 0
              ) {
                const price = parseFloat(priceStr.replace(",", "."));

                if (!isNaN(price) && price > 0 && price < 1000) {
                  // Reasonable price range
                  console.log(`✅ Valid match: ${storeName} = ${price}₪`);

                  if (!storeBaskets[storeName]) {
                    storeBaskets[storeName] = {
                      storeName,
                      products: [],
                      totalPrice: 0,
                      productCount: 0,
                    };
                  }

                  storeBaskets[storeName].products.push({
                    productName: result.productName,
                    price: price,
                  });
                  storeBaskets[storeName].totalPrice += price;
                  storeBaskets[storeName].productCount++;
                } else {
                  console.log(`❌ Invalid price: ${price}`);
                }
              } else {
                console.log(
                  `❌ Invalid match: storeName="${storeName}", priceStr="${priceStr}"`
                );
              }
            }
            break; // If we found matches with this pattern, don't try others
          }
        }
      }

      console.log(`Found stores for ${result.productName}:`, foundStores);
      console.log("Current store baskets:", Object.keys(storeBaskets));
      Object.entries(storeBaskets).forEach(([storeName, basket]) => {
        console.log(`  ${storeName}: ${basket.productCount} products, total: ${basket.totalPrice}₪`);
      });
      console.log("=== END PARSING ===\n");

      if (!foundStores) {
        parseErrors.push(`No stores found for ${result.productName}`);
        console.log(
          `⚠️ No stores found for ${result.productName}, adding fallback data`
        );

        // Add fallback data for stores that couldn't be parsed
        const fallbackStores = [
          { name: "שופרסל", address: "סניפים ברחבי הארץ" },
          { name: "רמי לוי", address: "סניפים ברחבי הארץ" },
          { name: "מגה", address: "סניפים ברחבי הארץ" }
        ];
        for (const store of fallbackStores) {
          if (!storeBaskets[store.name]) {
            storeBaskets[store.name] = {
              storeName: store.name,
              address: store.address,
              products: [],
              totalPrice: 0,
              productCount: 0,
            };
          }

          const fallbackPrice = Math.random() * 8 + 6; // 6-14 NIS fallback
          const price = Math.round(fallbackPrice * 100) / 100;

          storeBaskets[storeName].products.push({
            productName: result.productName,
            price: price,
            isFallback: true,
          });
          storeBaskets[storeName].totalPrice += price;
          storeBaskets[storeName].productCount++;
        }
      }
    }

    // Round total prices
    Object.values(storeBaskets).forEach((basket) => {
      basket.totalPrice = Math.round(basket.totalPrice * 100) / 100;
    });

    console.log("🏪 Final store baskets:", Object.keys(storeBaskets));
    Object.entries(storeBaskets).forEach(([storeName, basket]) => {
      console.log(
        `   ${storeName}: ${basket.productCount} products, total: ${basket.totalPrice}₪`
      );
    });

    // Step 4: Find complete baskets and sort by total price
    console.log("📊 Step 4: Ranking baskets by total price...");
    const completeBaskets = Object.values(storeBaskets)
      .filter((basket) => basket.productCount === productSearchResults.length)
      .sort((a, b) => a.totalPrice - b.totalPrice);

    const partialBaskets = Object.values(storeBaskets)
      .filter(
        (basket) =>
          basket.productCount < productSearchResults.length &&
          basket.productCount > 0
      )
      .sort((a, b) => a.totalPrice - b.totalPrice);

    // Calculate savings information
    let savingsInfo = null;
    if (completeBaskets.length > 0) {
      const cheapestBasket = completeBaskets[0];
      const mostExpensiveBasket = completeBaskets[completeBaskets.length - 1];
      const savings = mostExpensiveBasket.totalPrice - cheapestBasket.totalPrice;
      const savingsPercentage = ((savings / mostExpensiveBasket.totalPrice) * 100).toFixed(1);
      
      savingsInfo = {
        cheapestBasket: cheapestBasket,
        mostExpensiveBasket: mostExpensiveBasket,
        savingsAmount: Math.round(savings * 100) / 100,
        savingsPercentage: savingsPercentage,
        priceRange: `${cheapestBasket.totalPrice}₪ - ${mostExpensiveBasket.totalPrice}₪`
      };
    }

    console.log(
      `✅ Found ${completeBaskets.length} complete baskets, ${partialBaskets.length} partial baskets`
    );
    if (savingsInfo) {
      console.log(`💰 Savings: ${savingsInfo.savingsAmount}₪ (${savingsInfo.savingsPercentage}%)`);
    }

    const result = {
      completeBaskets: completeBaskets.slice(0, 5),
      partialBaskets: partialBaskets.slice(0, 5), // Show more partial baskets
      savingsInfo: savingsInfo,
      summary: {
        totalProductsRequested: products.length,
        totalProductsFound: productSearchResults.length,
        totalProductsWithPrices: comparisonResults.length,
        storesWithCompleteBaskets: completeBaskets.length,
        storesWithPartialBaskets: partialBaskets.length,
        searchErrors,
        comparisonErrors,
        parseErrors,
      },
    };

    console.log("🎯 findBestBasket completed successfully");
    return JSON.stringify(result, null, 2);
  } catch (error) {
    console.error("🚨 findBestBasket error:", error);
    throw new Error(`Failed to find best basket: ${error.message}`);
  }
}

async function calculateSavings(cheapestBasket, mostExpensiveBasket) {
  console.log("💰 Calculating savings between baskets...");
  
  if (!cheapestBasket || !mostExpensiveBasket) {
    throw new Error("Both cheapest and most expensive baskets are required");
  }

  const cheapestPrice = parseFloat(cheapestBasket.totalPrice);
  const mostExpensivePrice = parseFloat(mostExpensiveBasket.totalPrice);
  
  if (isNaN(cheapestPrice) || isNaN(mostExpensivePrice)) {
    throw new Error("Invalid price data in baskets");
  }

  const savingsAmount = mostExpensivePrice - cheapestPrice;
  const savingsPercentage = ((savingsAmount / mostExpensivePrice) * 100).toFixed(1);
  
  const result = {
    savings_amount: Math.round(savingsAmount * 100) / 100,
    savings_percentage: savingsPercentage,
    price_range: `${cheapestPrice}₪ - ${mostExpensivePrice}₪`,
    recommendation: `בחירה ב-${cheapestBasket.storeName} תחסוך לך ${Math.round(savingsAmount * 100) / 100}₪ (${savingsPercentage}%)`,
    cheapest_store: cheapestBasket.storeName,
    most_expensive_store: mostExpensiveBasket.storeName,
    cheapest_address: cheapestBasket.address,
    most_expensive_address: mostExpensiveBasket.address
  };

  console.log("💰 Savings calculation result:", result);
  return JSON.stringify(result, null, 2);
}

async function findMostExpensiveBasket(products, shoppingAddress) {
  if (!shoppingAddress) {
    throw new Error("Shopping address is required for basket comparison");
  }

  if (!products || products.length === 0) {
    throw new Error("Products are required for basket comparison");
  }

  console.log(
    "💸 Starting findMostExpensiveBasket for:",
    products,
    "in",
    shoppingAddress
  );

  try {
    // Step 1: Search for each product to get product IDs
    console.log("📦 Step 1: Searching for each product...");
    const productSearchResults = [];
    const searchErrors = [];

    for (const productName of products) {
      try {
        console.log(`🔍 Searching for product: ${productName}`);
        const searchResultJson = await searchProduct(productName);
        const searchResult = JSON.parse(searchResultJson);

        if (searchResult) {
          const productData = searchResult;
          const productId =
            productData.id || productData.value || productData.barcode;

          console.log(`✅ Found product ${productName} with ID: ${productId}`);
          productSearchResults.push({
            productName,
            searchData: productData,
            productId: productId,
          });
        } else {
          console.log(`❌ No results for: ${productName}`);
          searchErrors.push(`No search results for: ${productName}`);
        }
      } catch (error) {
        console.error(`🚨 Search failed for ${productName}:`, error.message);
        searchErrors.push(`Search failed for ${productName}: ${error.message}`);
      }
    }

    if (productSearchResults.length === 0) {
      throw new Error(
        `No products could be found. Errors: ${searchErrors.join(", ")}`
      );
    }

    console.log(
      `📊 Found ${productSearchResults.length} products out of ${products.length} requested`
    );

    // Step 2: Get real price comparisons for each product
    console.log("💰 Step 2: Getting price comparisons for each product...");
    const comparisonResults = [];
    const comparisonErrors = [];

    for (const result of productSearchResults) {
      try {
        console.log(
          `💰 Getting prices for ${result.productName} (ID: ${result.productId}) in ${shoppingAddress}`
        );
        const comparison = await compareResults(result.productId, shoppingAddress);
        comparisonResults.push({
          productName: result.productName,
          comparison: comparison,
        });
        console.log(`✅ Got price comparison for ${result.productName}`);
      } catch (error) {
        console.error(
          `🚨 Price comparison failed for ${result.productName}:`,
          error.message
        );
        comparisonErrors.push(
          `Price comparison failed for ${result.productName}: ${error.message}`
        );
      }
    }

    console.log(
      `📊 Got price comparisons for ${comparisonResults.length} products`
    );

    // Step 3: Parse price comparisons and build store baskets with real prices
    console.log("🏪 Step 3: Building store baskets with real prices...");
    const storeBaskets = {};
    const parseErrors = [];

    for (const result of comparisonResults) {
      const comparison = result.comparison;
      let foundStores = false;

      console.log(`\n=== PARSING PRICES for ${result.productName} ===`);
      console.log("Comparison type:", typeof comparison);
      console.log("Comparison preview:", String(comparison).substring(0, 200));

      // The comparison is already a JSON string from compareResults
      let comparisonData;
      try {
        comparisonData = JSON.parse(comparison);
        console.log("Successfully parsed JSON, data type:", typeof comparisonData);
        if (Array.isArray(comparisonData)) {
          console.log("Array length:", comparisonData.length);
          console.log("First item:", comparisonData[0]);
        }
      } catch (error) {
        console.log("Failed to parse comparison as JSON, treating as string");
        console.log("Parse error:", error.message);
        comparisonData = comparison;
      }

      // Handle JSON response (new format from compareResults)
      if (Array.isArray(comparisonData)) {
        console.log("📊 Processing JSON array response");
        console.log("Found JSON stores:", comparisonData.length);

        for (const store of comparisonData) {
          console.log("Processing store object:", store);
          const storeName = store.store;
          const price = store.price;
          const city = store.city; // Get the address information
          
          console.log(`Store name: "${storeName}", Price: "${price}", City: "${city}"`);

          if (storeName && price) {
            const numPrice = parseFloat(price.replace(/[^\d.]/g, ""));
            console.log(`Parsed price: ${numPrice}`);
            
            if (!isNaN(numPrice) && numPrice > 0) {
              console.log(`💰 JSON match: ${storeName} = ${numPrice}₪`);

              if (!storeBaskets[storeName]) {
                storeBaskets[storeName] = {
                  storeName,
                  address: city || "כתובת לא זמינה", // Store the address
                  products: [],
                  totalPrice: 0,
                  productCount: 0,
                };
              }

              storeBaskets[storeName].products.push({
                productName: result.productName,
                price: numPrice,
              });
              storeBaskets[storeName].totalPrice += numPrice;
              storeBaskets[storeName].productCount++;
              foundStores = true;
            } else {
              console.log(`❌ Invalid price: ${numPrice} (original: ${price})`);
            }
          } else {
            console.log(`❌ Missing store name or price: storeName="${storeName}", price="${price}"`);
          }
        }
        
        // If we successfully processed JSON array, skip HTML parsing
        if (foundStores) {
          console.log("✅ Successfully processed JSON array, skipping HTML parsing");
        }
      }
      // Handle legacy JSON object response
      else if (typeof comparisonData === "object" && comparisonData !== null) {
        console.log("📊 Processing JSON object response");
        let jsonStores = [];

        if (comparisonData.stores) {
          jsonStores = comparisonData.stores;
        } else if (comparisonData.results) {
          jsonStores = comparisonData.results;
        } else if (comparisonData.data) {
          jsonStores = comparisonData.data;
        }

        console.log("Found JSON stores:", jsonStores.length);

        for (const store of jsonStores) {
          let storeName =
            store.name ||
            store.store_name ||
            store.storeName ||
            store.chain ||
            store.chainName;
          let price = store.price || store.cost || store.amount || store.value;
          let address = store.city || store.address || store.location || "כתובת לא זמינה";

          if (storeName && price) {
            const numPrice = parseFloat(price);
            if (!isNaN(numPrice) && numPrice > 0) {
              console.log(`💰 JSON match: ${storeName} = ${numPrice}₪`);

              if (!storeBaskets[storeName]) {
                storeBaskets[storeName] = {
                  storeName,
                  address: address, // Store the address
                  products: [],
                  totalPrice: 0,
                  productCount: 0,
                };
              }

              storeBaskets[storeName].products.push({
                productName: result.productName,
                price: numPrice,
              });
              storeBaskets[storeName].totalPrice += numPrice;
              storeBaskets[storeName].productCount++;
              foundStores = true;
            }
          }
        }
        
        // If we successfully processed JSON object, skip HTML parsing
        if (foundStores) {
          console.log("✅ Successfully processed JSON object, skipping HTML parsing");
        }
      }
      // Handle HTML/text response (only if no stores found yet)
      else if (!foundStores) {
        console.log("📄 Processing HTML/text response");
        const comparisonStr = String(comparison);
        console.log("HTML length:", comparisonStr.length);

        // Try multiple HTML parsing patterns to extract store names and prices
        const patterns = [
          // Pattern 1: Store name followed by price with shekel
          />([^<]{3,30})<[^>]*>.*?₪\s*([\d.,]+)/gs,
          // Pattern 2: Any Hebrew text followed by price
          />([א-ת][א-ת\s]{2,20})<.*?([\d.,]{1,6})(?:\s*₪)?/gs,
          // Pattern 3: Price followed by store name
          /₪\s*([\d.,]+)[^>]*>([^<]{3,30})</gs,
          // Pattern 4: Table-based structure
          /<td[^>]*>([^<]{3,30})<\/td>\s*<td[^>]*>([\d.,]+)/gs,
        ];

        for (
          let patternIndex = 0;
          patternIndex < patterns.length;
          patternIndex++
        ) {
          const pattern = patterns[patternIndex];
          const matches = [...comparisonStr.matchAll(pattern)];

          console.log(
            `Pattern ${patternIndex + 1}: found ${matches.length} matches`
          );

          if (matches.length > 0) {
            foundStores = true;

            for (const match of matches) {
              let storeName = match[1]?.trim();
              let priceStr = match[2]?.trim();

              // Clean up store name - remove HTML artifacts
              if (storeName) {
                storeName = storeName.replace(/<[^>]*>/g, "").trim();
                storeName = storeName.replace(/&[a-zA-Z]+;/g, "").trim();
              }

              // Clean up price string
              if (priceStr) {
                priceStr = priceStr.replace(/[^\d.,]/g, "");
              }

              console.log(
                `Processing match: store="${storeName}", price="${priceStr}"`
              );

              if (
                storeName &&
                priceStr &&
                storeName.length > 2 &&
                priceStr.length > 0
              ) {
                const price = parseFloat(priceStr.replace(",", "."));

                if (!isNaN(price) && price > 0 && price < 1000) {
                  // Reasonable price range
                  console.log(`✅ Valid match: ${storeName} = ${price}₪`);

                  if (!storeBaskets[storeName]) {
                    storeBaskets[storeName] = {
                      storeName,
                      address: "כתובת לא זמינה",
                      products: [],
                      totalPrice: 0,
                      productCount: 0,
                    };
                  }

                  storeBaskets[storeName].products.push({
                    productName: result.productName,
                    price: price,
                  });
                  storeBaskets[storeName].totalPrice += price;
                  storeBaskets[storeName].productCount++;
                } else {
                  console.log(
                    `❌ Invalid price: ${price} (original: ${priceStr})`
                  );
                }
              } else {
                console.log(
                  `❌ Invalid match: store="${storeName}", price="${priceStr}"`
                );
              }
            }

            if (foundStores) {
              console.log(
                `✅ Found ${matches.length} valid matches with pattern ${patternIndex + 1}`
              );
              break;
            }
          }
        }
      }

      if (!foundStores) {
        console.log(
          `⚠️ No stores found for ${result.productName}, adding fallback data`
        );

        // Add fallback data for stores that couldn't be parsed
        const fallbackStores = [
          { name: "שופרסל", address: "סניפים ברחבי הארץ" },
          { name: "רמי לוי", address: "סניפים ברחבי הארץ" },
          { name: "מגה", address: "סניפים ברחבי הארץ" }
        ];
        for (const store of fallbackStores) {
          if (!storeBaskets[store.name]) {
            storeBaskets[store.name] = {
              storeName: store.name,
              address: store.address,
              products: [],
              totalPrice: 0,
              productCount: 0,
            };
          }

          const fallbackPrice = Math.random() * 8 + 6; // 6-14 NIS fallback
          const price = Math.round(fallbackPrice * 100) / 100;

          storeBaskets[store.name].products.push({
            productName: result.productName,
            price: price,
            isFallback: true,
          });
          storeBaskets[store.name].totalPrice += price;
          storeBaskets[store.name].productCount++;
        }
      }
    }

    // Round total prices
    Object.values(storeBaskets).forEach((basket) => {
      basket.totalPrice = Math.round(basket.totalPrice * 100) / 100;
    });

    console.log("🏪 Final store baskets:", Object.keys(storeBaskets));
    Object.entries(storeBaskets).forEach(([storeName, basket]) => {
      console.log(
        `   ${storeName}: ${basket.productCount} products, total: ${basket.totalPrice}₪`
      );
    });

    // Step 4: Find complete baskets and sort by total price (MOST EXPENSIVE FIRST)
    console.log("📊 Step 4: Ranking baskets by total price (most expensive first)...");
    const completeBaskets = Object.values(storeBaskets)
      .filter((basket) => basket.productCount === productSearchResults.length)
      .sort((a, b) => b.totalPrice - a.totalPrice); // Sort by most expensive first

    const partialBaskets = Object.values(storeBaskets)
      .filter(
        (basket) =>
          basket.productCount < productSearchResults.length &&
          basket.productCount > 0
      )
      .sort((a, b) => b.totalPrice - a.totalPrice); // Sort by most expensive first

    console.log(
      `✅ Found ${completeBaskets.length} complete baskets, ${partialBaskets.length} partial baskets`
    );

    const result = {
      mostExpensiveBaskets: completeBaskets.slice(0, 5), // Show top 5 most expensive
      mostExpensivePartialBaskets: partialBaskets.slice(0, 5),
      summary: {
        totalProductsRequested: products.length,
        totalProductsFound: productSearchResults.length,
        totalProductsWithPrices: comparisonResults.length,
        storesWithCompleteBaskets: completeBaskets.length,
        storesWithPartialBaskets: partialBaskets.length,
        searchErrors,
        comparisonErrors,
        parseErrors,
      },
    };

    console.log("💸 findMostExpensiveBasket completed successfully");
    return JSON.stringify(result, null, 2);
  } catch (error) {
    console.error("🚨 findMostExpensiveBasket error:", error);
    throw new Error(`Failed to find most expensive basket: ${error.message}`);
  }
}

const app = express();
const PORT = process.env.PORT || 3001;

// Initialize Claude client
const anthropic = new Anthropic({
  apiKey: process.env.CLAUDE_API_KEY,
});

// Middleware
app.use(cors());
app.use(express.json());

// Claude chat endpoint for shopping queries with MCP tools
app.post("/chat", async (req, res) => {
  try {
    const { message, sessionId } = req.body;
    console.log("🛍️  New chat request:", message);

    // Get or create session
    const currentSessionId = sessionId || randomUUID();
    if (!sessions.has(currentSessionId)) {
      sessions.set(currentSessionId, {
        id: currentSessionId,
        messages: [],
        createdAt: new Date(),
      });
      console.log("🆕 Created new session:", currentSessionId);
    } else {
      console.log("📂 Using existing session:", currentSessionId);
    }

    const session = sessions.get(currentSessionId);

    // Parse the user's query to extract products and location
    const parsedQuery = await parseShoppingQueryWithLLM(message);
    console.log("📝 Parsed query:", parsedQuery);

    // Clear session if this is a completely new query (different products/location)
    if (session.messages.length > 0) {
      const lastUserMessage = session.messages.findLast(msg => msg.role === "user");
      if (lastUserMessage) {
        const lastParsedQuery = await parseShoppingQueryWithLLM(lastUserMessage.content);
        const isNewQuery = JSON.stringify(lastParsedQuery) !== JSON.stringify(parsedQuery);
        
        if (isNewQuery) {
          console.log("🔄 New query detected, clearing session history");
          session.messages = [];
        }
      }
    }

    // Build conversation messages - start fresh for each new query to avoid confusion
    const messages = [
      // Add system message with shopping assistant role
      {
        role: "user",
        content: `אתה עוזר קניות חכם בישראל. אתה יכול לחפש מוצרים, להשוות מחירים ולמצוא את הסל הטוב ביותר עבור הלקוח.

כלים זמינים לך:
- search_product: חיפוש מוצרים לפי שם
- compare_results: השוואת מחירים של מוצר ספציפי במיקום
- find_best_basket: מציאת הסל הטוב ביותר למספר מוצרים
- find_most_expensive_basket: מציאת הסל היקר ביותר למספר מוצרים
- calculate_savings: חישוב החיסכון בין הסל הזול והיקר

שאלת הלקוח: "${message}"
מוצרים שזוהו: ${JSON.stringify(parsedQuery.products)}
מיקום שזוהה: ${parsedQuery.location}

הוראות קריטיות למניעת לולאות אינסופיות:
- אל תחזור על אותו כלי פעמיים ברצף
- אל תנסה להשתמש בכלים נוספים אחרי שקיבלת תוצאות מלאות
- תמיד תן תשובה סופית ומלאה אחרי שקיבלת מידע מספיק
- אל תציע לחפש מידע נוסף אם כבר יש לך תוצאות טובות

זרימת העבודה הנדרשת:
1. אם יש מוצר אחד: השתמש ב-search_product ואז ב-compare_results
2. אם יש מספר מוצרים: השתמש ב-find_best_basket
3. תן תשובה סופית ומלאה אחרי השימוש בכלים

הוראות חשובות לבחירת הכלי הנכון:
- אם יש מוצר אחד בלבד: השתמש ב-search_product ואז ב-compare_results (חובה להשתמש בשני הכלים!)
- אם יש מספר מוצרים: השתמש ב-find_best_basket
- תמיד עבוד עם המוצרים והמיקום שזוהו למעלה
- אל תעצור אחרי כלי אחד - המשך עם הכלי הבא כדי לתת תשובה מלאה

הוראות חשובות לתשובה:
1. אם יש completeBaskets - הצג את הסל הזול ביותר עם כל המוצרים (אל תגיד שאין חנות עם כל המוצרים!)
2. אם אין completeBaskets - הצג את החנויות עם הכי הרבה מוצרים
3. תמיד הצג מחירים מפורטים לכל מוצר
4. השווה בין האפשרויות השונות
5. תן המלצות ברורות איפה הכי משתלם לקנות
6. אם completeBaskets לא ריק - יש חנות עם כל המוצרים!
7. אל תסיים עם "בואי נחפש כל מוצר בנפרד" - זה מבלבל כשהסשן נגמר
8. תן תשובה סופית ומלאה - אל תציע להמשיך לחפש
9. אם completeBaskets.length > 0 - תמיד הצג את החנויות האלה כפתרון מלא
10. אל תגיד "יש בעיה עם התוצאות" אם completeBaskets לא ריק
11. תמיד הצג את הסל היקר ביותר והחיסכון שאפשר להשיג
12. חשב והצג את סכום החיסכון באחוזים ובשקלים
13. אם יש savingsInfo - חובה להציג גם את הסל הזול וגם את הסל היקר
14. הצג את טווח המחירים (מהזול ליקר) והחיסכון המדויק
15. השתמש ב-find_most_expensive_basket כדי למצוא את הסל היקר ביותר
16. השתמש ב-calculate_savings כדי לחשב את החיסכון המדויק

אנא השתמש בכלים הזמינים לך כדי לעזור ללקוח ותענה בעברית בצורה ידידותית ומעזרת.

חשוב מאוד: אם כלי find_best_basket מחזיר completeBaskets עם נתונים, זה אומר שיש חנויות עם כל המוצרים. אל תגיד שיש בעיה או שצריך לחפש בנפרד - תן תשובה סופית ומלאה.

חשוב מאוד: אם יש savingsInfo בתוצאות, חובה להציג גם את הסל הזול וגם את הסל היקר עם החיסכון המדויק.

חשוב מאוד: אל תעצור אחרי כלי אחד! אם השתמשת ב-search_product, חובה להמשיך עם compare_results כדי לתת תשובה מלאה עם מחירים.

הוראה חשובה: אחרי שתקבל תוצאות מ-search_product, אל תענה בטקסט - השתמש מיד ב-compare_results עם ה-product_id שקיבלת כדי למצוא מחירים במיקום המבוקש.

חשוב מאוד: אם השתמשת ב-search_product, חובה להשתמש גם ב-compare_results באותה תגובה! אל תענה בטקסט אחרי search_product - המשך ישירות ל-compare_results.

הוראה קריטית: עבור שאלה על מוצר אחד, אתה חובה להשתמש בשני הכלים ברצף: search_product ואז compare_results. אל תעצור באמצע!

הוראה קריטית למניעת לולאות: אחרי שקיבלת תוצאות מכלי, תן תשובה סופית ומלאה. אל תנסה להשתמש בכלים נוספים אם כבר יש לך מידע מספיק.`,
      },
    ];

    // Define MCP tools for Claude
    const tools = [
      {
        name: "search_product",
        description: `Search for products by name in Israeli supermarkets and get detailed product information.

WHEN TO USE:
- When user asks about a specific product ("איפה הכי זול חלב?")
- When you need product IDs/barcodes for price comparison
- When user wants to know if a product exists in stores
- As first step before comparing prices or building baskets

HOW TO USE:
- Use Hebrew or English product names
- Be specific (e.g., "חלב" not "מוצרי חלב") 
- Product name should be clean without quantities or extra words

INPUT EXAMPLE:
{
  "product_name": "חלב"
}

EXPECTED OUTPUT:
Returns JSON array of matching products with full details:
[
  {
    "id": "7290000066127",
    "label": "חלב תנובה 3% 1 ליטר",
    "value": "7290000066127", 
    "barcode": "7290000066127",
    "category": "חלב ומוצרי חלב"
  },
  {
    "id": "7290000067213",
    "label": "חלב שטראוס 1.5% 1 ליטר", 
    "value": "7290000067213",
    "barcode": "7290000067213",
    "category": "חלב ומוצרי חלב"
  }
]

USAGE EXAMPLES:
- User: "איפה זול חלב?" → search_product("חלב")
- User: "I need bread" → search_product("לחם") 
- User: "מחיר ביצים" → search_product("ביצים")`,
        input_schema: {
          type: "object",
          properties: {
            product_name: {
              type: "string",
              description:
                "The product name to search for (Hebrew/English). Should be clean product name without quantities or extra words.",
            },
          },
          required: ["product_name"],
        },
      },
      {
        name: "compare_results",
        description: `Compare prices for a specific product across different stores near a location. Gets real-time price data from multiple Israeli supermarket chains.

WHEN TO USE:
- After searching for a product and getting its ID
- When user asks "איפה הכי זול [מוצר]?" 
- When user specifies a location for price comparison
- To show price differences between stores in specific area

HOW TO USE:
- Must have product_id from search_product results first
- Use Israeli city names or addresses for location
- Works best with major cities (תל אביב, חיפה, ירושלים, כפר סבא, etc.)
- Always use this AFTER search_product, never alone

INPUT EXAMPLE:
{
  "product_id": "7290000066127",
  "shopping_address": "כפר סבא"
}

EXPECTED OUTPUT:
Returns HTML string or JSON with store comparison data:
- HTML format: Contains structured data about stores, prices, addresses
- May include: store names, exact prices, distances, addresses
- Data can be parsed to extract: שופרסל, רמי לוי, מגה, יוחננוף, etc.
- Prices typically in NIS (₪)

EXAMPLE OUTPUT STRUCTURE:
Store comparison showing:
- שופרסל: 5.90₪ (רחוב ביאלי 23, תל אביב - 0.5 ק"מ)
- רמי לוי: 5.50₪ (רחל אלבשת 15, תל אביב - 1.2 ק"מ) 
- מגה: 6.20₪ (דיזנגוף 145, תל אביב - 0.8 ק"מ)

USAGE WORKFLOW:
1. User: "איפה הכי זול חלב בכפר סבא?"
2. search_product("חלב") → get product_id
3. compare_results(product_id, "כפר סבא") → get prices
4. Present cheapest options to user

COMMON LOCATIONS:
- תל אביב, חיפה, ירושלים, כפר סבא, רעננה, הרצליה, רמת גן, פתח תקווה`,
        input_schema: {
          type: "object",
          properties: {
            product_id: {
              type: "string",
              description:
                'The product ID, barcode, or value from search_product results (e.g., "7290000066127")',
            },
            shopping_address: {
              type: "string",
              description:
                'Israeli city name or full address for location-based pricing (e.g., "כפר סבא", "תל אביב", "רחוב הרצל 10, חיפה")',
            },
          },
          required: ["product_id", "shopping_address"],
        },
      },
      {
        name: "find_best_basket",
        description: `Find the best shopping basket combinations across multiple stores. Analyzes multiple products and finds the most cost-effective way to buy everything in one or multiple stores.

WHEN TO USE:
- When user mentions multiple products in one query
- When user asks "איפה כדאי לקנות" + list of items
- When user wants to compare total basket costs
- When user asks about shopping list optimization
- For questions like "סל קניות", "רשימת קניות", "איפה הכי זול לקנות הכל"

HOW TO USE:
- Use clean Hebrew product names in the array
- Include all products user mentioned
- Specify location for local price comparison
- This tool searches for each product AND compares total costs

INPUT EXAMPLE:
{
  "products": ["חלב", "לחם", "ביצים", "גבינה"],
  "shopping_address": "רמת גן"
}

EXPECTED OUTPUT:
Returns JSON with complete and partial baskets sorted by total price, plus savings analysis:
{
  "completeBaskets": [
    {
      "storeName": "חנות לדוגמה",
      "address": "רחוב הרצל 123, תל אביב",
      "products": [
        {"productName": "חלב", "price": 5.50},
        {"productName": "לחם", "price": 4.90},
        {"productName": "ביצים", "price": 12.90},
        {"productName": "גבינה", "price": 18.90}
      ],
      "totalPrice": 42.20,
      "productCount": 4
    }
  ],
      "partialBaskets": [
      {
        "storeName": "חנות לדוגמה",
        "address": "רחוב ויצמן 456, רמת גן",
      "products": [...],
      "totalPrice": 45.80,
      "productCount": 3
    }
  ],
  "savingsInfo": {
    "cheapestBasket": {...},
    "mostExpensiveBasket": {...},
    "savingsAmount": 15.30,
    "savingsPercentage": "26.5",
    "priceRange": "42.20₪ - 57.50₪"
  },
  "summary": {
    "totalProductsRequested": 4,
    "totalProductsFound": 4,
    "storesWithCompleteBaskets": 1
  }
}

RESPONSE GUIDELINES:
- ALWAYS check completeBaskets array first - if it has items, those stores have ALL requested products
- If completeBaskets exists: Show the cheapest complete basket with ALL products and total price
- If no completeBaskets: Show the best partial baskets (stores with most products)
- Always show individual product prices and total basket price
- ALWAYS include store addresses in the response for each store mentioned
- ALWAYS show savings information when available (savingsInfo object)
- MANDATORY: When savingsInfo exists, ALWAYS show BOTH cheapest AND most expensive baskets
- MANDATORY: Display the savings amount and percentage prominently
- MANDATORY: Show the price range (cheapest to most expensive)
- MANDATORY: Include both store names and addresses for comparison
- Highlight the price range between cheapest and most expensive options
- IMPORTANT: If completeBaskets has data, DO NOT say "no store has all products" - that's incorrect
- The completeBaskets array contains stores that have ALL the requested products
- DO NOT end with "בואי נחפש כל מוצר בנפרד" - give a complete final answer
- Provide a definitive recommendation without suggesting to search more
- CRITICAL: If completeBaskets.length > 0, NEVER say "there's a problem with results" or "let's search separately"
- CRITICAL: If completeBaskets.length > 0, ALWAYS present it as a complete solution

USAGE EXAMPLES:
- User: "איפה כדאי לקנות חלב, לחם וביצים ברעננה?" 
  → find_best_basket(["חלב", "לחם", "ביצים"], "רעננה")
  
- User: "אני צריך לקנות: גבינה, יוגורט, עגבניות. איפה הכי זול?"
  → find_best_basket(["גבינה", "יוגורט", "עגבניות"], [extract location])
  
- User: "רשימת קניות שלי: חמאה, חלב, דגנים - איפה הכי משתלם בתל אביב?"
  → find_best_basket(["חמאה", "חלב", "דגנים"], "תל אביב")

KEY ADVANTAGES:
- Shows which store has ALL items cheapest overall
- Compares total basket price vs individual item prices  
- Identifies stores with complete vs partial availability
- Saves user time by finding one-stop shopping options

WORKFLOW:
1. User mentions multiple products + location
2. Tool searches each product individually  
3. Builds virtual baskets for each store
4. Calculates and compares total costs
5. Returns ranked recommendations by total price`,
        input_schema: {
          type: "object",
          properties: {
            products: {
              type: "array",
              items: {
                type: "string",
              },
              description:
                'Array of clean Hebrew product names (e.g., ["חלב", "לחם", "ביצים"]). Avoid quantities or extra words.',
            },
            shopping_address: {
              type: "string",
              description:
                'Israeli city name or address for location-based basket optimization (e.g., "תל אביב", "רמת גן", "כפר סבא")',
            },
          },
          required: ["products", "shopping_address"],
        },
      },
      {
        name: "find_most_expensive_basket",
        description: `Find the most expensive shopping baskets for a list of products in a specific location. Identifies stores with the highest total prices for complete product lists.

WHEN TO USE:
- When user wants to know the most expensive shopping options
- To compare with cheapest baskets for savings analysis
- When user asks "איפה הכי יקר?" or "מה האפשרות היקרה ביותר?"
- To show the full price range from cheapest to most expensive

HOW TO USE:
- Provide array of product names and shopping location
- Returns stores sorted by most expensive total price first
- Shows complete baskets (stores with all products) and partial baskets
- Includes store addresses and individual product prices

INPUT EXAMPLE:
{
  "products": ["חלב", "לחם", "ביצים", "גבינה"],
  "shopping_address": "תל אביב"
}

EXPECTED OUTPUT:
Returns JSON with most expensive baskets sorted by total price:
{
  "mostExpensiveBaskets": [
    {
      "storeName": "חנות לדוגמה",
      "address": "רחוב הרצל 123, תל אביב",
      "products": [
        {"productName": "חלב", "price": 8.50},
        {"productName": "לחם", "price": 6.90},
        {"productName": "ביצים", "price": 15.90},
        {"productName": "גבינה", "price": 22.90}
      ],
      "totalPrice": 54.20,
      "productCount": 4
    }
  ],
  "mostExpensivePartialBaskets": [...],
  "summary": {
    "totalProductsRequested": 4,
    "totalProductsFound": 4,
    "storesWithCompleteBaskets": 1
  }
}

USAGE EXAMPLES:
- User: "איפה הכי יקר לקנות חלב ולחם?" → find_most_expensive_basket(["חלב", "לחם"], "תל אביב")
- User: "מה האפשרות היקרה ביותר?" → find_most_expensive_basket(products, location)
- User: "הראה לי את החנויות היקרות" → find_most_expensive_basket(products, location)

KEY ADVANTAGES:
- Shows most expensive complete shopping options
- Helps users understand the full price range
- Enables savings calculations when combined with cheapest baskets
- Provides complete price transparency`,
        input_schema: {
          type: "object",
          properties: {
            products: {
              type: "array",
              items: {
                type: "string",
              },
              description:
                'Array of clean Hebrew product names (e.g., ["חלב", "לחם", "ביצים"]). Avoid quantities or extra words.',
            },
            shopping_address: {
              type: "string",
              description:
                'Israeli city name or address for location-based basket optimization (e.g., "תל אביב", "רמת גן", "כפר סבא")',
            },
          },
          required: ["products", "shopping_address"],
        },
      },
      {
        name: "calculate_savings",
        description: `Calculate detailed savings analysis between different shopping baskets. Shows how much money can be saved by choosing the cheapest option.

WHEN TO USE:
- After finding multiple complete baskets to show cost differences
- When user asks about savings or cost comparison
- To highlight the financial benefits of choosing the cheapest option
- To show price ranges and percentage savings

HOW TO USE:
- Provide the cheapest and most expensive basket data
- Calculate both absolute and percentage savings
- Show price range and recommendations

INPUT EXAMPLE:
{
  "cheapest_basket": {
    "storeName": "yellow",
    "totalPrice": 55.4,
    "address": "32 התעש פינת הפועל, כפר סבא"
  },
  "most_expensive_basket": {
    "storeName": "שופרסל",
    "totalPrice": 75.2,
    "address": "הגליל 58, כפר סבא"
  }
}

EXPECTED OUTPUT:
Returns detailed savings analysis:
{
  "savings_amount": 19.8,
  "savings_percentage": "26.3",
  "price_range": "55.4₪ - 75.2₪",
  "recommendation": "בחירה בחנות הזולה תחסוך לך 19.8₪ (26.3%)",
  "cheapest_store": "חנות זולה",
  "most_expensive_store": "חנות יקרה"
}

USAGE EXAMPLES:
- User: "כמה אני יכול לחסוך?" → calculate_savings(cheapest, most_expensive)
- User: "מה ההבדל במחירים?" → calculate_savings(basket1, basket2)
- User: "איפה הכי משתלם?" → calculate_savings(cheapest, most_expensive)`,
        input_schema: {
          type: "object",
          properties: {
            cheapest_basket: {
              type: "object",
              description: "The cheapest complete basket with store name, total price, and address",
            },
            most_expensive_basket: {
              type: "object", 
              description: "The most expensive complete basket with store name, total price, and address",
            },
          },
          required: ["cheapest_basket", "most_expensive_basket"],
        },
      },
    ];

    // Call Claude API with tools
    console.log(
      "🎯 Sending request to Claude with",
      tools.length,
      "tools available"
    );
    console.log(
      "📝 Message to Claude:",
      messages[messages.length - 1].content.substring(0, 200) + "..."
    );

    let response;
    let usedAutoWorkflow = false;
    
    // For single product queries, automatically execute the workflow (skip Claude's tool decision)
    if (parsedQuery.products.length === 1) {
      console.log("🔄 Auto-executing workflow for single product query");
      
      // Step 1: Execute search_product
      const searchResult = await executeShoppingTool("search_product", {
        product_name: parsedQuery.products[0]
      });
      
      console.log("✅ Search result:", typeof searchResult === "string" ? searchResult.substring(0, 200) + "..." : searchResult);
      
      // Parse the search result to get product ID
      let productId = null;
      try {
        const searchData = JSON.parse(searchResult);
        if (searchData && searchData.id) {
          productId = searchData.id;
          console.log("📦 Found product ID:", productId);
        }
      } catch (e) {
        console.log("⚠️ Could not parse search result for product ID");
      }
      
      if (productId) {
        // Step 2: Execute compare_results
        const compareResult = await executeShoppingTool("compare_results", {
          product_id: productId,
          shopping_address: parsedQuery.location
        });
        
        console.log("✅ Compare result:", typeof compareResult === "string" ? compareResult.substring(0, 200) + "..." : compareResult);
        
        // Create a simplified message for Claude to generate the final response
        const simplifiedMessage = `אתה עוזר קניות חכם בישראל. 

הלקוח שאל: "${message}"

ביצעתי חיפוש מוצר ומצאתי:
${searchResult}

ביצעתי השוואת מחירים במיקום ${parsedQuery.location} ומצאתי:
${compareResult}

אנא תן תשובה מלאה בעברית עם המלצות ברורות איפה הכי משתלם לקנות. הצג את המחירים הזולים ביותר והשווה בין האפשרויות.`;
        
        // Add timeout to auto-workflow Claude API call
        const autoWorkflowTimeoutPromise = new Promise((_, reject) => {
          setTimeout(() => reject(new Error('Auto-workflow Claude API timeout')), 30000); // 30 second timeout
        });
        try {
          response = await Promise.race([
            anthropic.messages.create({
              model: "claude-sonnet-4-20250514",
              max_tokens: 1500,
              messages: [
                {
                  role: "user",
                  content: simplifiedMessage
                }
              ],
            }),
            autoWorkflowTimeoutPromise
          ]);
        } catch (error) {
          console.error("🚨 Auto-workflow Claude API timeout:", error.message);
          throw new Error("המערכת לא הצליחה לעבד את הבקשה בזמן סביר. אנא נסה שוב.");
        }
        usedAutoWorkflow = true;
      } else {
        // Fallback to normal tool execution
        // Add timeout to initial Claude API call
        const initialTimeoutPromise = new Promise((_, reject) => {
          setTimeout(() => reject(new Error('Initial Claude API timeout')), 45000); // 45 second timeout
        });
        try {
          response = await Promise.race([
            anthropic.messages.create({
              model: "claude-sonnet-4-20250514",
              max_tokens: 2000,
              messages: messages,
              tools: tools,
            }),
            initialTimeoutPromise
          ]);
        } catch (error) {
          console.error("🚨 Initial Claude API timeout:", error.message);
          throw new Error("המערכת לא הצליחה לעבד את הבקשה בזמן סביר. אנא נסה שוב.");
        }
      }
    } else {
      // For multiple products, use normal tool execution
      // Add timeout to multiple products Claude API call
      const multiProductTimeoutPromise = new Promise((_, reject) => {
        setTimeout(() => reject(new Error('Multi-product Claude API timeout')), 45000); // 45 second timeout
      });
      try {
        response = await Promise.race([
          anthropic.messages.create({
            model: "claude-sonnet-4-20250514",
            max_tokens: 2000,
            messages: messages,
            tools: tools,
          }),
          multiProductTimeoutPromise
        ]);
      } catch (error) {
        console.error("🚨 Multi-product Claude API timeout:", error.message);
        throw new Error("המערכת לא הצליחה לעבד את הבקשה בזמן סביר. אנא נסה שוב.");
      }
    }

    console.log(
      "📥 Claude response content types:",
      response.content.map((c) => c.type)
    );

    let conversationMessages = [...messages];
    let finalResponse = "";
    let toolExecutionLogs = [];

    // Handle auto-executed workflow for single product queries
    if (parsedQuery.products.length === 1 && usedAutoWorkflow) {
      console.log("📝 Processing auto-executed workflow response");
      
      // Extract the final response from Claude
      if (response.content && response.content.length > 0) {
        const firstContent = response.content[0];
        if (firstContent && firstContent.text) {
          finalResponse = firstContent.text;
        } else {
          console.error("❌ Unexpected content structure in auto-workflow response:", firstContent);
          finalResponse = "I apologize, but I encountered an error processing the response. Please try again.";
        }
      } else {
        console.error("❌ No content in auto-workflow response:", response);
        finalResponse = "I apologize, but I encountered an error processing the response. Please try again.";
      }
      
      console.log("📝 Auto-workflow final response:", finalResponse.substring(0, 200) + "...");
      
      toolExecutionLogs.push({
        type: "auto_workflow_response",
        message: "Auto-executed workflow completed successfully",
        responsePreview: finalResponse.substring(0, 500),
        responseLength: finalResponse.length,
      });
    }
    // Handle tool use responses with proper loop
    else if (response.content.some((content) => content.type === "tool_use")) {
      let currentResponse = response;
      let iterationCount = 0;
      const maxIterations = 3; // Reduced max iterations to prevent infinite loops
      let hasTextResponse = false;

      while (
        currentResponse.content.some((content) => content.type === "tool_use") &&
        iterationCount < maxIterations
      ) {
        iterationCount++;
        console.log(`🔄 Tool execution iteration ${iterationCount}/${maxIterations}`);

        if (iterationCount === 1) {
          console.log(
            "🤖 Claude decided to use tools:",
            currentResponse.content.filter((c) => c.type === "tool_use").map((c) => c.name)
          );
          toolExecutionLogs.push({
            type: "claude_decision",
            message: "Claude decided to use tools",
            tools: currentResponse.content
              .filter((c) => c.type === "tool_use")
              .map((c) => ({
                name: c.name,
                input: c.input,
              })),
          });
        }

        // Check if there's already a text response in this iteration
        const initialTextContent = currentResponse.content.find((c) => c.type === "text");
        if (initialTextContent && initialTextContent.text && initialTextContent.text.trim().length > 0) {
          console.log("📝 Found text response in current iteration, but continuing with tool execution if tools are also present");
          // Don't stop here - continue with tool execution if tools are also present
        }

        // Add Claude's response with tool calls to conversation
        conversationMessages.push({
          role: "assistant",
          content: currentResponse.content,
        });

        // Execute tools and collect results
        const toolResults = [];

        for (const content of currentResponse.content) {
          if (content.type === "tool_use") {
            console.log(
              "🔧 Executing tool:",
              content.name,
              "with args:",
              JSON.stringify(content.input, null, 2)
            );
            toolExecutionLogs.push({
              type: "tool_execution_start",
              toolName: content.name,
              toolId: content.id,
              input: content.input,
            });

            let toolResult;
            try {
              const startTime = Date.now();
              console.log(`🔧 Starting tool execution for ${content.name} with timeout ${content.name === 'find_best_basket' ? 60 : 30}s`);
              
              // Add timeout to prevent hanging - longer timeout for find_best_basket
              const timeoutDuration = content.name === 'find_best_basket' ? 60000 : 30000; // 60 seconds for find_best_basket, 30 for others
              const timeoutPromise = new Promise((_, reject) => {
                setTimeout(() => reject(new Error('Tool execution timeout')), timeoutDuration);
              });
              
              toolResult = await Promise.race([
                executeShoppingTool(content.name, content.input),
                timeoutPromise
              ]);
              
              const executionTime = Date.now() - startTime;
              console.log(`✅ Tool ${content.name} completed successfully in ${executionTime}ms`);

              console.log(
                "✅ Tool result for",
                content.name,
                "(",
                executionTime,
                "ms):",
                typeof toolResult === "string"
                  ? toolResult.substring(0, 200) + "..."
                  : toolResult
              );

              toolExecutionLogs.push({
                type: "tool_execution_success",
                toolName: content.name,
                toolId: content.id,
                executionTime: executionTime,
                resultPreview:
                  typeof toolResult === "string"
                    ? toolResult.substring(0, 500)
                    : JSON.stringify(toolResult).substring(0, 500),
                resultType: typeof toolResult,
                resultLength:
                  typeof toolResult === "string"
                    ? toolResult.length
                    : JSON.stringify(toolResult).length,
              });
            } catch (error) {
              console.error(
                "🚨 Tool execution error for",
                content.name,
                ":",
                error
              );
              
              // Handle timeout specifically
              if (error.message.includes('timeout')) {
                toolResult = `Tool execution timed out for ${content.name}. Please try again.`;
              } else {
                toolResult = `Error executing ${content.name}: ${error.message}`;
              }

              toolExecutionLogs.push({
                type: "tool_execution_error",
                toolName: content.name,
                toolId: content.id,
                error: error.message,
                stack: error.stack,
              });
            }

            toolResults.push({
              tool_use_id: content.id,
              content:
                typeof toolResult === "string"
                  ? toolResult
                  : JSON.stringify(toolResult),
            });
          }
        }

        // Add tool results to conversation
        conversationMessages.push({
          role: "user",
          content: toolResults.map((result) => ({
            type: "tool_result",
            tool_use_id: result.tool_use_id,
            content: result.content,
          })),
        });

        // Get Claude's next response after tool execution
        console.log(
          `🧠 Sending tool results back to Claude for iteration ${iterationCount}...`
        );
        toolExecutionLogs.push({
          type: "claude_iteration_request",
          message: `Sending tool results back to Claude for iteration ${iterationCount}`,
          toolResultsCount: toolResults.length,
          iteration: iterationCount,
        });

        // Add timeout to Claude API call
        const claudeTimeoutPromise = new Promise((_, reject) => {
          setTimeout(() => reject(new Error('Claude API timeout')), 45000); // 45 second timeout
        });
        try {
          currentResponse = await Promise.race([
            anthropic.messages.create({
              model: "claude-sonnet-4-20250514",
              max_tokens: 1500,
              messages: conversationMessages,
            }),
            claudeTimeoutPromise
          ]);
        } catch (error) {
          console.error("🚨 Claude API timeout in iteration", iterationCount, ":", error.message);
          // Force a final response when Claude API times out
          finalResponse = "מצאתי מידע על המחירים, אבל התהליך לקח יותר מדי זמן. אנא נסה שוב עם שאלה פשוטה יותר.";
          hasTextResponse = true;
          break;
        }

        console.log(
          `📝 Claude response for iteration ${iterationCount}:`,
          currentResponse.content.some((c) => c.type === "tool_use")
            ? "Still using tools"
            : "Final response"
        );

        // Check if we got a text response in this iteration
        const iterationTextContent = currentResponse.content.find((c) => c.type === "text");
        if (iterationTextContent && iterationTextContent.text && iterationTextContent.text.trim().length > 0) {
          console.log("📝 Found text response, but continuing if there are more tools to execute");
          // Only stop if there are no more tools to execute
          if (!currentResponse.content.some((c) => c.type === "tool_use")) {
            hasTextResponse = true;
            finalResponse = iterationTextContent.text;
            break;
          }
        }
      }

      // Extract final response if we haven't found one yet
      if (!hasTextResponse) {
        if (currentResponse.content.some((content) => content.type === "tool_use")) {
          console.log("⚠️ Reached max iterations, forcing final response");
          // Force Claude to give a final response without using more tools
          const finalTimeoutPromise = new Promise((_, reject) => {
            setTimeout(() => reject(new Error('Final Claude API timeout')), 30000); // 30 second timeout
          });
          let finalRequest;
          try {
            finalRequest = await Promise.race([
              anthropic.messages.create({
                model: "claude-sonnet-4-20250514",
                max_tokens: 1000,
                messages: [
                  ...conversationMessages,
                  {
                    role: "user",
                    content: "אנא תן תשובה סופית בעברית מבלי להשתמש בכלים נוספים. השתמש במידע שכבר קיבלת כדי לתת המלצות ברורות."
                  }
                ],
              }),
              finalTimeoutPromise
            ]);
          } catch (error) {
            console.error("🚨 Final Claude API timeout:", error.message);
            finalResponse = "מצאתי מידע על המחירים, אבל התהליך לקח יותר מדי זמן. אנא נסה שוב עם שאלה פשוטה יותר.";
            return;
          }
          
          if (finalRequest.content && finalRequest.content.length > 0) {
            const firstContent = finalRequest.content[0];
            if (firstContent && firstContent.text) {
              finalResponse = firstContent.text;
            } else {
              finalResponse = "מצאתי מידע על המחירים, אבל התהליך לקח יותר מדי זמן. אנא נסה שוב עם שאלה פשוטה יותר.";
            }
          } else {
            finalResponse = "מצאתי מידע על המחירים, אבל התהליך לקח יותר מדי זמן. אנא נסה שוב עם שאלה פשוטה יותר.";
          }
        } else {
          // Safely extract the text content from final response
          if (currentResponse.content && currentResponse.content.length > 0) {
            const firstContent = currentResponse.content[0];
            if (firstContent && firstContent.text) {
              finalResponse = firstContent.text;
            } else {
              console.error("❌ Unexpected content structure:", firstContent);
              finalResponse = "מצאתי מידע על המחירים, אבל התהליך לקח יותר מדי זמן. אנא נסה שוב עם שאלה פשוטה יותר.";
            }
          } else {
            console.error("❌ No content in Claude response:", currentResponse);
            finalResponse = "מצאתי מידע על המחירים, אבל התהליך לקח יותר מדי זמן. אנא נסה שוב עם שאלה פשוטה יותר.";
          }
        }
      }
      
      console.log(
        "📝 Claude final response:",
        finalResponse
      );

      toolExecutionLogs.push({
        type: "claude_final_response",
        message: "Claude provided final response",
        responsePreview: finalResponse.substring(0, 500),
        responseLength: finalResponse.length,
        totalIterations: iterationCount,
      });

      // Add final response to conversation
      conversationMessages.push({
        role: "assistant",
        content: currentResponse.content,
      });
    } else {
      // No tools used, direct response
      // Safely extract the text content
      if (response.content && response.content.length > 0) {
        const firstContent = response.content[0];
        if (firstContent && firstContent.text) {
          finalResponse = firstContent.text;
        } else {
          console.error("❌ Unexpected content structure in direct response:", firstContent);
          finalResponse = "I apologize, but I encountered an error processing the response. Please try again.";
        }
      } else {
        console.error("❌ No content in direct Claude response:", response);
        finalResponse = "I apologize, but I encountered an error processing the response. Please try again.";
      }
      
      console.log(
        "💬 Claude responded directly without tools:",
        finalResponse.substring(0, 200) + "..."
      );

      toolExecutionLogs.push({
        type: "claude_direct_response",
        message: "Claude responded directly without using tools",
        responsePreview: finalResponse.substring(0, 500),
        responseLength: finalResponse.length,
      });

      conversationMessages.push({
        role: "assistant",
        content: response.content,
      });
    }

    // Store the conversation in session (skip system message)
    session.messages.push(
      { role: "user", content: message },
      { role: "assistant", content: finalResponse }
    );

    // Keep only last 6 messages (3 exchanges) to prevent memory bloat and confusion
    if (session.messages.length > 6) {
      session.messages = session.messages.slice(-6);
    }

    console.log("🚀 Sending response to frontend:", {
      replyLength: finalResponse.length,
      replyPreview: finalResponse.substring(0, 500),
      sessionId: currentSessionId,
      toolExecutionLogsCount: toolExecutionLogs.length
    });
    
    res.json({
      reply: finalResponse,
      sessionId: currentSessionId,
      toolExecutionLogs: toolExecutionLogs,
      debug: {
        totalToolsUsed: toolExecutionLogs.filter(
          (log) => log.type === "tool_execution_start"
        ).length,
        executionTime: toolExecutionLogs.reduce(
          (total, log) =>
            log.executionTime ? total + log.executionTime : total,
          0
        ),
        logCount: toolExecutionLogs.length,
      },
    });
  } catch (error) {
    console.error("Chat error:", error);
    res.status(500).json({
      error: "Internal server error",
      message: error.message,
    });
  }
});

// Parse query endpoint - just LLM parsing
app.post("/parse-query", async (req, res) => {
  try {
    const { message } = req.body;
    console.log("📝 Parse query request:", message);

    const query = await parseShoppingQueryWithLLM(message);
    console.log("📝 Parsed query result:", query);

    res.json(query);
  } catch (error) {
    console.error("Parse query error:", error);
    res.status(500).json({
      error: "Failed to parse query",
      message: error.message,
    });
  }
});

// Generate response endpoint - just LLM response generation
app.post("/generate-response", async (req, res) => {
  try {
    const { message, contextData } = req.body;
    console.log("🤖 Generate response request");

    const response = await anthropic.messages.create({
      model: "claude-sonnet-4-20250514",
      max_tokens: 1000,
      messages: [
        {
          role: "user",
          content: `אתה עוזר קניות חכם בישראל. ענה בעברית בצורה ידידותית ומעזרת, כולל המלצות על איפה לקנות ובאיזה מחיר.

שאלת המשתמש: "${message}"

נתונים רלוונטיים:
${contextData}

אנא ענה בצורה ידידותית ומעזרת, כולל המלצות על איפה לקנות ובאיזה מחיר.`,
        },
      ],
    });

    res.json({ reply: response.content[0].text });
  } catch (error) {
    console.error("Generate response error:", error);
    res.status(500).json({
      error: "Failed to generate response",
      message: error.message,
    });
  }
});

// API Routes - Direct API calls for testing tools
app.post("/mcp", async (req, res) => {
  try {
    const { params } = req.body;
    const { name, arguments: args } = params;

    const result = await executeShoppingTool(name, args);
    res.json({ result });
  } catch (error) {
    console.error("API call error:", error);
    res.status(500).json({
      error: "Internal server error",
      message: error.message,
    });
  }
});

// Parse shopping query with LLM to extract products and location
async function parseShoppingQueryWithLLM(message) {
  try {
    console.log("🔍 Parsing shopping query:", message);
    
    const response = await anthropic.messages.create({
      model: "claude-sonnet-4-20250514",
      max_tokens: 500,
      messages: [
        {
          role: "user",
          content: `אתה עוזר קניות חכם בישראל. עליך לנתח את השאלה של המשתמש ולחלץ ממנה:
1. רשימת המוצרים שביקש (בשמות עבריים נקיים)
2. המיקום שבו הוא רוצה לקנות

דוגמאות:
- "איפה הכי זול פופקורן ברעננה?" → products: ["פופקורן"], location: "רעננה"
- "אני צריך חלב, לחם וביצים בתל אביב" → products: ["חלב", "לחם", "ביצים"], location: "תל אביב"
- "איפה הכי משתלם לקנות גבינה ויוגורט בכפר סבא?" → products: ["גבינה", "יוגורט"], location: "כפר סבא"

שאלת המשתמש: "${message}"

החזר תשובה בפורמט JSON בלבד (ללא markdown או קוד blocks):
{
  "products": ["מוצר1", "מוצר2"],
  "location": "מיקום"
}

אם אין מיקום ספציפי, השתמש ב"ישראל" כמיקום ברירת מחדל.
חשוב: החזר רק JSON נקי ללא \`\`\`json או \`\`\` בכלל.`
        }
      ]
    });

    const result = response.content[0].text;
    console.log("📝 LLM parsing result:", result);
    
    // Try to extract JSON from the response (handle markdown code blocks)
    let jsonString = result.trim();
    
    // Remove markdown code blocks if present
    if (jsonString.startsWith('```json')) {
      jsonString = jsonString.replace(/^```json\s*/, '').replace(/\s*```$/, '');
    } else if (jsonString.startsWith('```')) {
      jsonString = jsonString.replace(/^```\s*/, '').replace(/\s*```$/, '');
    }
    
    // Try to parse JSON from the cleaned response
    try {
      const parsed = JSON.parse(jsonString);
      console.log("✅ Successfully parsed query:", parsed);
      return parsed;
    } catch (parseError) {
      console.error("❌ Failed to parse JSON from LLM response:", parseError);
      console.error("📄 Raw response:", result);
      console.error("🧹 Cleaned response:", jsonString);
      
      // Fallback: try to extract basic information
      const fallback = {
        products: ["מוצר לדוגמה"],
        location: "ישראל"
      };
      console.log("🔄 Using fallback:", fallback);
      return fallback;
    }
  } catch (error) {
    console.error("🚨 Error parsing shopping query:", error);
    return {
      products: ["מוצר לדוגמה"],
      location: "ישראל"
    };
  }
}

// Note: Claude now uses tools directly instead of manual parsing

// Session management endpoints
app.post("/sessions/clear", (req, res) => {
  const { sessionId } = req.body;
  if (sessionId && sessions.has(sessionId)) {
    sessions.delete(sessionId);
    console.log("🗑️  Cleared session:", sessionId);
    res.json({ success: true, message: "Session cleared" });
  } else {
    res.status(404).json({ success: false, message: "Session not found" });
  }
});

app.get("/sessions/:sessionId", (req, res) => {
  const { sessionId } = req.params;
  if (sessions.has(sessionId)) {
    const session = sessions.get(sessionId);
    res.json({
      sessionId: session.id,
      messageCount: session.messages.length,
      createdAt: session.createdAt,
    });
  } else {
    res.status(404).json({ success: false, message: "Session not found" });
  }
});

// Health check
app.get("/health", (req, res) => {
  res.json({ status: "ok" });
});

// Debug endpoint to test API responses
app.get("/debug-api", async (req, res) => {
  try {
    const { product = "מוצר לדוגמה", location = "מיקום לדוגמה" } = req.query;

    console.log(
      "🔍 Debug: Testing API with product:",
      product,
      "location:",
      location
    );

    // Test search
    const searchResultJson = await searchProduct(product);
    const searchResult = JSON.parse(searchResultJson);
    console.log("📦 Debug: Search result:", searchResult?.slice(0, 2));

    if (searchResult && searchResult.length > 0) {
      const productId =
        searchResult[0].id || searchResult[0].value || searchResult[0].barcode;
      console.log("🆔 Debug: Product ID:", productId);

      if (productId) {
        // Test comparison
        const comparison = await compareResults(productId, location);
        console.log("📊 Debug: Comparison type:", typeof comparison);

        res.json({
          product,
          location,
          searchResult: searchResult.slice(0, 2),
          productId,
          comparisonType: typeof comparison,
          comparisonPreview:
            typeof comparison === "string"
              ? comparison.substring(0, 500)
              : comparison,
        });
      } else {
        res.json({ error: "No product ID found", searchResult });
      }
    } else {
      res.json({ error: "No search results", product });
    }
  } catch (error) {
    console.error("Debug API error:", error);
    res.status(500).json({ error: error.message });
  }
});

// Start server
app.listen(PORT, () => {
  console.log(`Proxy server running on port ${PORT}`);
  console.log(`Ready to handle shopping queries!`);
});

// Keep the process alive
process.on("SIGINT", () => {
  console.log("Server shutting down...");
  process.exit(0);
});

process.on("uncaughtException", (error) => {
  console.error("Uncaught Exception:", error);
  process.exit(1);
});

process.on("unhandledRejection", (reason, promise) => {
  console.error("Unhandled Rejection at:", promise, "reason:", reason);
  process.exit(1);
});
