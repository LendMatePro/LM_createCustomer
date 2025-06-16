import { TransactWriteItemsCommand } from "@aws-sdk/client-dynamodb";
import { marshall } from "@aws-sdk/util-dynamodb";
import { ddbClient } from "./ddbClient.js";
import kuuid from "kuuid";

export const handler = async (event) => {
    try {


        const payload = JSON.parse(event.body);

        console.log("Payload:", payload);

        const { name, address, email, phone } = payload;

        const result = await createCustomer(name, address, email, phone);
        if (!result.status) {
            return {
                statusCode: 400,
                headers: {
                    "Access-Control-Allow-Origin": "*"
                },
                body: JSON.stringify({
                    message: result.message
                })
            };
        }

        return {
            statusCode: 200,
            body: JSON.stringify({
                message: "Customer details saved successfully",
                customerId: result.customerId
            }),
            headers: {
                "Access-Control-Allow-Origin": "*"
            }
        };
    } catch (error) {
        console.error("Handler Error:", error);
        return {
            statusCode: 400,
            headers: {
                "Access-Control-Allow-Origin": "*"
            },
            body: error.message
        };
    }
};

async function createCustomer(name, address, email, phone) {
    const returnValue = { status: false, message: null, customerId: null };

    try {
        
        const customerId = kuuid.id({ random: 4, millisecond: true });
        const normalizedName = name.trim().toUpperCase().replace(/\s+/g, "_");

        // Main record
        const PK_main = "CUSTOMER";
        const SK_main = customerId;

        // Search record
        const PK_search = "CUSTOMER_LOOKUP";
        const SK_search = `${phone}#${normalizedName}`;

        // Lock record
        const PK_lock = `CUSTOMER_PHONE#${phone}`;
        const SK_lock = "LOCK";

        // Transaction
        const input = {
            TransactItems: [
                {
                    Put: {
                        TableName: process.env.DYNAMODB_TABLE_NAME,
                        Item: marshall({
                            PK: PK_main,
                            SK: SK_main,
                            name,
                            address,
                            email,
                            phone
                        }),
                        ConditionExpression: "attribute_not_exists(PK)"
                    }
                },
                {
                    Put: {
                        TableName: process.env.DYNAMODB_TABLE_NAME,
                        Item: marshall({
                            PK: PK_search,
                            SK: SK_search,
                            Info: {
                                customerId,
                                name,
                                email,
                                address,
                                phone
                            }
                        }),
                        ConditionExpression: "attribute_not_exists(PK)"
                    }
                },
                {
                    Put: {
                        TableName: process.env.DYNAMODB_TABLE_NAME,
                        Item: marshall({
                            PK: PK_lock,
                            SK: SK_lock,
                            customerId
                        }),
                        ConditionExpression: "attribute_not_exists(PK)"
                    }
                }
            ]
        };

        const command = new TransactWriteItemsCommand(input);
        await ddbClient.send(command);

        returnValue.status = true;
        returnValue.customerId = customerId;
        return returnValue;

    } catch (error) {
        console.error("Transaction Error:", error);
        returnValue.message = error.name === "TransactionCanceledException"
            ? "Phone number already exists"
            : error.message;
        return returnValue;
    }
}
