import dotenv from "dotenv";

dotenv.config();

export class ShoppingMCPServer {
  constructor() {}

  getToolsDefinition() {
    return [
      {
        name: "search_product",
        description:
          "Search for products by name in Israeli supermarkets. Returns detailed product information including IDs, barcodes, and metadata.",
        inputSchema: {
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
        description:
          "Compare prices for a specific product across different stores near a location. Gets price comparison data from multiple Israeli supermarket chains.",
        inputSchema: {
          type: "object",
          properties: {
            product_id: {
              type: "string",
              description: "The product ID or barcode (from search results)",
            },
            shopping_address: {
              type: "string",
              description: "Israeli city or address for location-based pricing",
            },
          },
          required: ["product_id", "shopping_address"],
        },
      },
      {
        name: "find_best_basket",
        description:
          "Find the best shopping basket combinations across multiple stores. Analyzes multiple products and finds the most cost-effective shopping baskets by comparing total prices across different Israeli supermarket chains.",
        inputSchema: {
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
              description:
                "Israeli city or address for location-based basket optimization",
            },
          },
          required: ["products", "shopping_address"],
        },
      },
    ];
  }

  async executeToolByName(toolName, args) {
    switch (toolName) {
      case "search_product":
        return await this.handleSearchProduct(args.product_name);
      case "compare_results":
        return await this.handleCompareResults(
          args.product_id,
          args.shopping_address
        );
      case "find_best_basket":
        return await this.handleFindBestBasket(
          args.products,
          args.shopping_address
        );
      default:
        throw new Error(`Unknown tool: ${toolName}`);
    }
  }

  setupToolHandlers() {
    this.server.setRequestHandler(ListToolsRequestSchema, async () => {
      return {
        tools: this.getToolsDefinition(),
      };
    });

    this.server.setRequestHandler(CallToolRequestSchema, async (request) => {
      const { name, arguments: args } = request.params;

      try {
        return await this.executeToolByName(name, args);
      } catch (error) {
        return {
          content: [
            {
              type: "text",
              text: `Error: ${error.message}`,
            },
          ],
          isError: true,
        };
      }
    });
  }

  async handleSearchProduct(productName) {
    if (!productName || productName.trim() === "") {
      throw new Error("Product name is required for search");
    }

    console.log(
      "🔍 MCP Server: Searching for product in Salim API:",
      productName
    );

    const fetch = (await import("node-fetch")).default;
    const encodedTerm = encodeURIComponent(productName.trim());
    const apiUrl = `http://localhost:8000/products?q=${encodedTerm}&limit=10`;

    const response = await fetch(apiUrl);
    if (!response.ok) {
      throw new Error(
        `Product search failed: ${response.status} ${response.statusText}`
      );
    }

    const results = await response.json();
    console.log(
      "📦 MCP Server: Found",
      results?.length || 0,
      "products in Salim database"
    );

    // Transform results to include useful information for shopping comparison
    const transformedResults = results.map((product) => ({
      id: product.product_id,
      barcode: product.barcode,
      name: product.canonical_name,
      brand: product.brand,
      category: product.category,
      price: product.price,
      promo_price: product.promo_price,
      promo_text: product.promo_text,
      supermarket_id: product.supermarket_id,
      size_value: product.size_value,
      size_unit: product.size_unit,
      currency: product.currency,
      in_stock: product.in_stock,
    }));

    return {
      content: [
        {
          type: "text",
          text: JSON.stringify(transformedResults, null, 2),
        },
      ],
    };
  }

  async handleCompareResults(productId, shoppingAddress) {
    if (!productId || productId.toString().trim() === "") {
      throw new Error("Product ID or barcode is required for price comparison");
    }

    console.log(
      "💰 MCP Server: Comparing prices in Salim API for product:",
      productId,
      "near:",
      shoppingAddress
    );

    const fetch = (await import("node-fetch")).default;

    let apiUrl;
    let comparisonData;

    try {
      apiUrl = `http://localhost:8000/products/barcode/${encodeURIComponent(
        productId.toString().trim()
      )}`;
      const response = await fetch(apiUrl);

      if (response.ok) {
        comparisonData = await response.json();
        console.log(
          "📊 MCP Server: Found price comparison by barcode across",
          comparisonData?.length || 0,
          "supermarkets"
        );
      } else {
        throw new Error("No barcode match, trying product search");
      }
    } catch (error) {
      console.log(
        "🔍 MCP Server: Barcode lookup failed, trying product ID search"
      );
      try {
        const productResponse = await fetch(
          `http://localhost:8000/products/${productId}`
        );
        if (productResponse.ok) {
          const product = await productResponse.json();
          const barcodeResponse = await fetch(
            `http://localhost:8000/products/barcode/${product.barcode}`
          );
          if (barcodeResponse.ok) {
            comparisonData = await barcodeResponse.json();
            console.log(
              "📊 MCP Server: Found price comparison by product ID->barcode across",
              comparisonData?.length || 0,
              "supermarkets"
            );
          } else {
            throw new Error("No price comparison data available");
          }
        } else {
          throw new Error("Product not found");
        }
      } catch (searchError) {
        throw new Error(`Price comparison failed: ${searchError.message}`);
      }
    }

    const transformedComparison = {
      product_name: comparisonData[0]?.canonical_name || "Unknown Product",
      brand: comparisonData[0]?.brand || "",
      category: comparisonData[0]?.category || "",
      barcode: comparisonData[0]?.barcode || "",
      size_info: `${comparisonData[0]?.size_value || ""} ${
        comparisonData[0]?.size_unit || ""
      }`.trim(),
      shopping_location: shoppingAddress,
      price_comparison: comparisonData.map((item) => ({
        supermarket: item.supermarket_name,
        price: item.price,
        promo_price: item.promo_price,
        promo_text: item.promo_text,
        savings: item.savings,
        in_stock: item.in_stock,
        currency: "ILS",
      })),
      best_price: Math.min(
        ...comparisonData.map((item) => item.promo_price || item.price)
      ),
      cheapest_store: comparisonData[0]?.supermarket_name, // Already sorted by price
      total_stores_checked: comparisonData.length,
      comparison_timestamp: new Date().toISOString(),
    };

    return {
      content: [
        {
          type: "text",
          text: JSON.stringify(transformedComparison, null, 2),
        },
      ],
    };
  }

  async handleFindBestBasket(products, shoppingAddress) {
    if (!shoppingAddress) {
      throw new Error("Shopping address is required for basket comparison");
    }

    if (!products || products.length === 0) {
      throw new Error("Products are required for basket comparison");
    }

    console.log(
      "🏪 MCP Server: Finding best basket using Salim API for products:",
      products,
      "near:",
      shoppingAddress
    );

    try {
      const fetch = (await import("node-fetch")).default;

      // Step 1: Search for each product and collect all results
      const productSearchResults = [];
      const searchErrors = [];

      for (const productName of products) {
        try {
          const searchResponse = await this.handleSearchProduct(productName);
          const searchResult = JSON.parse(searchResponse.content[0].text);

          if (searchResult && searchResult.length > 0) {
            // Find the best match (exact name match or first result)
            const bestMatch =
              searchResult.find(
                (p) =>
                  p.name.toLowerCase().includes(productName.toLowerCase()) ||
                  productName.toLowerCase().includes(p.name.toLowerCase())
              ) || searchResult[0];

            productSearchResults.push({
              productName,
              product: bestMatch,
            });
          } else {
            searchErrors.push(`No search results for: ${productName}`);
          }
        } catch (error) {
          searchErrors.push(
            `Search failed for ${productName}: ${error.message}`
          );
        }
      }

      if (productSearchResults.length === 0) {
        throw new Error(
          `No products could be found. Errors: ${searchErrors.join(", ")}`
        );
      }

      // Step 2: Get price comparisons for each product using barcodes
      const basketData = {};
      const comparisonErrors = [];

      for (const [id, name] of Object.entries(supermarketNames)) {
        basketData[name] = {
          supermarket_id: parseInt(id),
          supermarket_name: name,
          products: [],
          totalPrice: 0,
          totalPromoPrice: 0,
          totalSavings: 0,
          productCount: 0,
          location: shoppingAddress,
        };
      }

      // Process each product
      for (const productResult of productSearchResults) {
        try {
          // Get price comparison for this product's barcode
          const comparisonUrl = `http://localhost:8000/products/barcode/${productResult.product.barcode}`;
          const response = await fetch(comparisonUrl);

          if (response.ok) {
            const priceComparison = await response.json();

            // Add this product to each store's basket
            for (const priceData of priceComparison) {
              const storeName = priceData.supermarket_name;
              if (basketData[storeName]) {
                const effectivePrice = priceData.promo_price || priceData.price;
                const savings = priceData.savings || 0;

                basketData[storeName].products.push({
                  name: priceData.canonical_name,
                  brand: priceData.brand,
                  category: priceData.category,
                  barcode: priceData.barcode,
                  regular_price: priceData.price,
                  promo_price: priceData.promo_price,
                  effective_price: effectivePrice,
                  savings: savings,
                  promo_text: priceData.promo_text,
                  size_info: `${priceData.size_value || ""} ${
                    priceData.size_unit || ""
                  }`.trim(),
                  in_stock: priceData.in_stock,
                });

                basketData[storeName].totalPrice += parseFloat(priceData.price);
                basketData[storeName].totalPromoPrice +=
                  parseFloat(effectivePrice);
                basketData[storeName].totalSavings += savings || 0;
                basketData[storeName].productCount++;
              }
            }
          } else {
            comparisonErrors.push(
              `Price comparison failed for ${productResult.productName}`
            );
          }
        } catch (error) {
          comparisonErrors.push(
            `Error processing ${productResult.productName}: ${error.message}`
          );
        }
      }

      // Step 3: Calculate final results and sort by best value
      const completeBaskets = Object.values(basketData)
        .filter((basket) => basket.productCount === productSearchResults.length) // Only baskets with all products
        .map((basket) => ({
          ...basket,
          totalPrice: Math.round(basket.totalPrice * 100) / 100,
          totalPromoPrice: Math.round(basket.totalPromoPrice * 100) / 100,
          totalSavings: Math.round(basket.totalSavings * 100) / 100,
          averagePricePerProduct:
            Math.round((basket.totalPromoPrice / basket.productCount) * 100) /
            100,
        }))
        .sort((a, b) => a.totalPromoPrice - b.totalPromoPrice); // Sort by effective price

      const result = {
        basket_comparison: completeBaskets,
        best_basket: completeBaskets[0] || null,
        shopping_location: shoppingAddress,
        summary: {
          total_products_requested: products.length,
          total_products_found: productSearchResults.length,
          stores_with_complete_baskets: completeBaskets.length,
          best_total_price: completeBaskets[0]?.totalPromoPrice || 0,
          worst_total_price:
            completeBaskets[completeBaskets.length - 1]?.totalPromoPrice || 0,
          max_potential_savings:
            completeBaskets.length > 1
              ? Math.round(
                  (completeBaskets[completeBaskets.length - 1].totalPromoPrice -
                    completeBaskets[0].totalPromoPrice) *
                    100
                ) / 100
              : 0,
          search_errors: searchErrors,
          comparison_errors: comparisonErrors,
        },
        comparison_timestamp: new Date().toISOString(),
      };

      return {
        content: [
          {
            type: "text",
            text: JSON.stringify(result, null, 2),
          },
        ],
      };
    } catch (error) {
      throw new Error(`Failed to find best basket: ${error.message}`);
    }
  }

  setupErrorHandling() {
    this.server.onerror = (error) => {
      console.error("[MCP Error]", error);
    };

    process.on("SIGINT", async () => {
      await this.server.close();
      process.exit(0);
    });
  }
}
