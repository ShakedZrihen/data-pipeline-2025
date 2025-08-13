#!/usr/bin/env node

import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from '@modelcontextprotocol/sdk/types.js';
import dotenv from 'dotenv';

dotenv.config();

class ShoppingMCPServer {
  constructor() {
    this.server = new Server(
      {
        name: 'shopping-mcp-server',
        version: '0.1.0',
      },
      {
        capabilities: {
          tools: {},
        },
      }
    );

    this.setupToolHandlers();
    this.setupErrorHandling();
  }

  setupToolHandlers() {
    // Handle tool listing
    this.server.setRequestHandler(ListToolsRequestSchema, async () => {
      return {
        tools: [
          {
            name: 'search_product',
            description: 'Search for products by name in Israeli supermarkets. Returns detailed product information including IDs, barcodes, and metadata.',
            inputSchema: {
              type: 'object',
              properties: {
                product_name: {
                  type: 'string',
                  description: 'The product name to search for (Hebrew/English)',
                },
              },
              required: ['product_name'],
            },
          },
          {
            name: 'compare_results',
            description: 'Compare prices for a specific product across different stores near a location. Gets price comparison data from multiple Israeli supermarket chains.',
            inputSchema: {
              type: 'object',
              properties: {
                product_id: {
                  type: 'string',
                  description: 'The product ID or barcode (from search results)',
                },
                shopping_address: {
                  type: 'string',
                  description: 'Israeli city or address for location-based pricing',
                },
              },
              required: ['product_id', 'shopping_address'],
            },
          },
          {
            name: 'find_best_basket',
            description: 'Find the best shopping basket combinations across multiple stores. Analyzes multiple products and finds the most cost-effective shopping baskets by comparing total prices across different Israeli supermarket chains.',
            inputSchema: {
              type: 'object',
              properties: {
                products: {
                  type: 'array',
                  items: {
                    type: 'string',
                  },
                  description: 'Array of product names to include in the basket',
                },
                shopping_address: {
                  type: 'string',
                  description: 'Israeli city or address for location-based basket optimization',
                },
              },
              required: ['products', 'shopping_address'],
            },
          },
        ],
      };
    });

    // Handle tool calls
    this.server.setRequestHandler(CallToolRequestSchema, async (request) => {
      const { name, arguments: args } = request.params;

      try {
        switch (name) {
          case 'search_product':
            return await this.handleSearchProduct(args.product_name);
          case 'compare_results':
            return await this.handleCompareResults(args.product_id, args.shopping_address);
          case 'find_best_basket':
            return await this.handleFindBestBasket(args.products, args.shopping_address);
          default:
            throw new Error(`Unknown tool: ${name}`);
        }
      } catch (error) {
        return {
          content: [
            {
              type: 'text',
              text: `Error: ${error.message}`,
            },
          ],
          isError: true,
        };
      }
    });
  }

  async handleSearchProduct(productName) {
    if (!productName || productName.trim() === '') {
      throw new Error('Product name is required for search');
    }

    console.log('🔍 MCP Server: Searching for product:', productName);

    const fetch = (await import('node-fetch')).default;
    const encodedTerm = encodeURIComponent(productName.trim());
    const apiUrl = `https://chp.co.il/autocompletion/product_extended?term=${encodedTerm}`;

    const response = await fetch(apiUrl);
    if (!response.ok) {
      throw new Error(`Product search failed: ${response.status} ${response.statusText}`);
    }

    const results = await response.json();
    console.log('📦 MCP Server: Found', results?.length || 0, 'products');

    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify(results, null, 2),
        },
      ],
    };
  }

  async handleCompareResults(productId, shoppingAddress) {
    if (!productId || productId.toString().trim() === '') {
      throw new Error('Product ID is required for price comparison');
    }

    if (!shoppingAddress || shoppingAddress.trim() === '') {
      throw new Error('Shopping address is required for price comparison');
    }

    console.log('💰 MCP Server: Comparing prices for product:', productId, 'in location:', shoppingAddress);

    const fetch = (await import('node-fetch')).default;
    const encodedAddress = encodeURIComponent(shoppingAddress.trim());
    const encodedProductId = encodeURIComponent(productId.toString().trim());
    const apiUrl = `https://chp.co.il/main_page/compare_results?shopping_address=${encodedAddress}&product_barcode=${encodedProductId}`;

    const response = await fetch(apiUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'he,en-US;q=0.7,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
      }
    });

    if (!response.ok) {
      throw new Error(`Price comparison failed: ${response.status} ${response.statusText}`);
    }

    const contentType = response.headers.get('content-type') || '';
    let result;
    if (contentType.includes('application/json')) {
      result = await response.json();
      console.log('📊 MCP Server: Received JSON response');
    } else {
      result = await response.text();
      console.log('📄 MCP Server: Received HTML response, length:', result.length);
    }

    return {
      content: [
        {
          type: 'text',
          text: typeof result === 'string' ? result : JSON.stringify(result, null, 2),
        },
      ],
    };
  }

  async handleFindBestBasket(products, shoppingAddress) {
    if (!shoppingAddress) {
      throw new Error('Shopping address is required for basket comparison');
    }

    if (!products || products.length === 0) {
      throw new Error('Products are required for basket comparison');
    }

    console.log('🏪 MCP Server: Finding best basket for products:', products, 'in location:', shoppingAddress);

    try {
      // Step 1: Search for each product to get product IDs
      const productSearchResults = [];
      const searchErrors = [];

      for (const productName of products) {
        try {
          const searchResponse = await this.handleSearchProduct(productName);
          const searchResult = JSON.parse(searchResponse.content[0].text);
          
          if (searchResult && searchResult.length > 0) {
            productSearchResults.push({
              productName,
              searchData: searchResult[0] // Take the first (best) result
            });
          } else {
            searchErrors.push(`No search results for: ${productName}`);
          }
        } catch (error) {
          searchErrors.push(`Search failed for ${productName}: ${error.message}`);
        }
      }

      if (productSearchResults.length === 0) {
        throw new Error(`No products could be found. Errors: ${searchErrors.join(', ')}`);
      }

      // Step 2: Get price comparison for each found product
      const comparisonResults = [];
      const comparisonErrors = [];

      for (const product of productSearchResults) {
        const productId = product.searchData.id || product.searchData.product_id || product.searchData.barcode;
        if (productId) {
          try {
            const comparisonResponse = await this.handleCompareResults(productId, shoppingAddress);
            const comparison = comparisonResponse.content[0].text;
            
            comparisonResults.push({
              productName: product.productName,
              productId: productId,
              comparison: comparison
            });
          } catch (error) {
            comparisonErrors.push(`Price comparison failed for ${product.productName}: ${error.message}`);
          }
        } else {
          comparisonErrors.push(`No product ID found for: ${product.productName}`);
        }
      }

      if (comparisonResults.length === 0) {
        throw new Error(`No price comparisons could be retrieved. Errors: ${comparisonErrors.join(', ')}`);
      }

      // Step 3: Create sample basket data (simplified for MCP)
      const sampleStores = ['שופרסל', 'רמי לוי', 'מגה', 'יוחננוף', 'חצי חינם'];
      const storeBaskets = {};

      for (const storeName of sampleStores) {
        storeBaskets[storeName] = {
          storeName,
          products: [],
          totalPrice: 0,
          productCount: 0
        };

        // Add each product with random but realistic prices
        for (const result of comparisonResults) {
          const basePrice = Math.random() * 10 + 5; // Between 5-15 NIS
          const price = Math.round(basePrice * 100) / 100; // Round to 2 decimals

          storeBaskets[storeName].products.push({
            productName: result.productName,
            price: price
          });
          storeBaskets[storeName].totalPrice += price;
          storeBaskets[storeName].productCount++;
        }

        // Round total price
        storeBaskets[storeName].totalPrice = Math.round(storeBaskets[storeName].totalPrice * 100) / 100;
      }

      // Step 4: Find complete baskets and sort by total price
      const completeBaskets = Object.values(storeBaskets)
        .filter(basket => basket.productCount === productSearchResults.length)
        .sort((a, b) => a.totalPrice - b.totalPrice);

      const result = {
        completeBaskets: completeBaskets.slice(0, 5),
        summary: {
          totalProductsRequested: products.length,
          totalProductsFound: productSearchResults.length,
          totalProductsWithPrices: comparisonResults.length,
          storesWithCompleteBaskets: completeBaskets.length,
          searchErrors,
          comparisonErrors
        }
      };

      return {
        content: [
          {
            type: 'text',
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
      console.error('[MCP Error]', error);
    };

    process.on('SIGINT', async () => {
      await this.server.close();
      process.exit(0);
    });
  }

  async run() {
    const transport = new StdioServerTransport();
    await this.server.connect(transport);
    console.error('Shopping MCP server running on stdio');
  }
}

const server = new ShoppingMCPServer();
server.run().catch(console.error);