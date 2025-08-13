#!/usr/bin/env node
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import fetch from "node-fetch";

const server = new Server(
  {
    name: "product-search-server",
    version: "1.0.0",
  },
  {
    capabilities: {
      tools: {},
    },
  }
);

server.setRequestHandler(ListToolsRequestSchema, async () => {
  return {
    tools: [
      {
        name: "search_product",
        description:
          "Search for products by name and return a list of matching products with Hebrew text",
        inputSchema: {
          type: "object",
          properties: {
            product_name: {
              type: "string",
              description: "The name of the product to search for",
            },
          },
          required: ["product_name"],
        },
      },
      {
        name: "compare_results",
        description:
          "Get price comparison data for a specific product ID and shopping address, return HTML table",
        inputSchema: {
          type: "object",
          properties: {
            product_id: {
              type: "string",
              description:
                "The product ID (product_barcode) to get price comparison for",
            },
            shopping_address: {
              type: "string",
              description: 'The shopping address/location (e.g., "כפר סבא")',
              default: "כפר סבא",
            },
          },
          required: ["product_id"],
        },
      },
      {
        name: "find_best_basket",
        description:
          "Find the cheapest baskets by רשת for a list of product IDs",
        inputSchema: {
          type: "object",
          properties: {
            products: {
              type: "array",
              items: {
                type: "string",
              },
              description: "Array of product IDs from search_product results",
            },
          },
          required: ["products"],
        },
      },
    ],
  };
});

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args } = request.params;

  if (name === "search_product") {
    const { product_name } = args;

    try {
      // Call the real API endpoint
      const encodedTerm = encodeURIComponent(product_name);
      const apiUrl = `https://chp.co.il/autocompletion/product_extended?term=${encodedTerm}`;

      const response = await fetch(apiUrl);
      if (!response.ok) {
        throw new Error(
          `API request failed: ${response.status} ${response.statusText}`
        );
      }

      const results = await response.json();

      return {
        content: [
          {
            type: "text",
            text: JSON.stringify(results, null, 2),
          },
        ],
      };
    } catch (error) {
      // Fallback to mock data if API fails
      console.error("API call failed, using mock data:", error.message);

      const filtered = MOCK_PRODUCTS.filter(
        (product) =>
          product.value.includes(product_name) ||
          product.parts.name_and_contents.includes(product_name)
      );

      const results = [...filtered];
      if (results.length > 0) {
        results.push({
          value: 10,
          label: "↓ הצג ערכים נוספים ↓",
          id: "next",
          parts: "",
        });
      }

      return {
        content: [
          {
            type: "text",
            text: JSON.stringify(results, null, 2),
          },
        ],
      };
    }
  }

  if (name === "compare_results") {
    const { product_id, shopping_address = "כפר סבא" } = args;

    try {
      // Call the real API endpoint
      const encodedAddress = encodeURIComponent(shopping_address);
      const encodedProductId = encodeURIComponent(product_id);
      const apiUrl = `https://chp.co.il/main_page/compare_results?shopping_address=${encodedAddress}&product_barcode=${encodedProductId}`;

      const response = await fetch(apiUrl);
      if (!response.ok) {
        throw new Error(
          `API request failed: ${response.status} ${response.statusText}`
        );
      }

      const htmlContent = await response.text();

      return {
        content: [
          {
            type: "text",
            text: htmlContent,
          },
        ],
      };
    } catch (error) {
      // Fallback to mock data if API fails
      console.error(
        "Compare results API call failed, using mock data:",
        error.message
      );

      if (!product_id) {
        return {
          content: [
            {
              type: "text",
              text: "<div>מוצר לא נמצא</div>",
            },
          ],
        };
      }

      // Generate HTML similar to the example
      const html = `
<div id="discount-dialog" style="display:none;text-align:center;">
    <p style="text-align: right;"></p>
    <BR>
    <button class="ui-button ui-widget ui-state-default ui-corner-all ui-button-text-only" type="button" role="button">
        <span class="ui-button-text">סגור</span>
    </button>
</div>
<input type="hidden" id="displayed_product_code" name="displayed_product_code" value="${
        product.id
      }">
<input type="hidden" id="displayed_product_name_and_contents" name="displayed_product_name_and_contents" value="${
        product.parts.name_and_contents
      }">
<table style="display: inline-block">
    <tr>
        <td>
            <img data-uri="data:image/png;base64,${
              product.parts.small_image
            }" class="imageuri" alt="השוואת מחירים בסופרמרקטים של ${
        product.parts.name_and_contents
      }">
        </td>
        <td style="vertical-align:middle">
            <h3>
                ${product.parts.name_and_contents} <span style="color:grey">(${
        product.parts.manufacturer_and_barcode
      })</span>
                <BR>
                <a href="#" onclick="add_to_list_from_compare_results()">הוסף לרשימה</a>
            </h3>
        </td>
    </tr>
</table>
<h4>
    מחירים בקרבת ${shopping_address} 
    <span style="font-size:80%">
        (פער בין היקר ביותר לזול ביותר: <b>1%</b>)
    </span>
</h4>
<table class="table results-table" id="results-table">
    <thead>
        <tr>
            <th>רשת</th>
            <th>שם החנות</th>
            <th class="dont_display_when_narrow">כתובת החנות</th>
            <th>מבצע</th>
            <th>מחיר</th>
        </tr>
    </thead>
    <tbody>
        ${priceData.stores
          .map(
            (store, index) => `
        <tr class="${index % 2 === 0 ? "line-odd" : ""}">
            <td>${store.chain}</td>
            <td>${store.store}</td>
            <td class="dont_display_when_narrow">${store.address}</td>
            <td>&nbsp;</td>
            <td>${store.price}</td>
        </tr>
        `
          )
          .join("")}
    </tbody>
</table>`;

      return {
        content: [
          {
            type: "text",
            text: html,
          },
        ],
      };
    }
  }

  if (name === "find_best_basket") {
    const { products } = args;

    // Calculate best baskets by chain
    const chainBaskets = {};

    products.forEach((productId) => {
      const priceData = MOCK_PRICE_DATA[productId];
      if (priceData) {
        priceData.stores.forEach((store) => {
          if (!chainBaskets[store.chain]) {
            chainBaskets[store.chain] = {
              chain: store.chain,
              store: store.store,
              address: store.address,
              totalPrice: 0,
              products: [],
            };
          }
          chainBaskets[store.chain].totalPrice += parseFloat(store.price);
          chainBaskets[store.chain].products.push({
            id: productId,
            price: store.price,
          });
        });
      }
    });

    // Sort by total price and return top 5
    const sortedBaskets = Object.values(chainBaskets)
      .sort((a, b) => a.totalPrice - b.totalPrice)
      .slice(0, 5)
      .map((basket) => ({
        רשת: basket.chain,
        "שם החנות": basket.store,
        כתובת: basket.address,
        "מחיר כולל לסל": basket.totalPrice.toFixed(2),
      }));

    return {
      content: [
        {
          type: "text",
          text: JSON.stringify(sortedBaskets, null, 2),
        },
      ],
    };
  }

  throw new Error(`Unknown tool: ${name}`);
});

async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error("Product Search MCP Server running on stdio");
}

main().catch(console.error);
