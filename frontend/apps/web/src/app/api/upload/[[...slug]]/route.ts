import { Server } from "@tus/server";
import { S3Store } from "@tus/s3-store";
import type { NextRequest } from "next/server";

const s3Store = new S3Store({
  partSize: 8 * 1024 * 1024,
  s3ClientConfig: {
    endpoint: process.env.AWS_URL!,
    bucket: process.env.AWS_BUCKET!,
    region: process.env.AWS_REGION!,
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!,
    },
    forcePathStyle: true,
  },
});

const server = new Server({
  path: "/api/upload",
  datastore: s3Store,
  async onUploadCreate(req, upload) {
    if (upload.metadata?.filetype !== "text/csv") {
      throw {
        status_code: 400,
        body: "Only CSV files are allowed. Please upload a file with 'text/csv' content type.",
      };
    }

    const MAX_CSV_SIZE = 50 * 1024 * 1024; // 50 MB
    if (upload.size && upload.size > MAX_CSV_SIZE) {
      throw { status_code: 413, body: `CSV file too large. Max size is ${MAX_CSV_SIZE / (1024 * 1024)}MB.` };
    }

    return { metadata: upload.metadata };
  },
  // Add custom error handler
  async onUploadFinish(req, upload) {
    console.log(`Upload finished for file: ${upload.id}`);
    console.log("File location (key in MinIO):", upload.id);

    try {
      console.log("Starting CSV processing for file:", upload.id);

      // The rest of the code will never be reached
      // Create an AbortController for timeout
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 30000); // 30 second timeout

      const requestBody = JSON.stringify({
        s3_key: upload.id,
        table_name: upload.id,
      });

      console.log("Sending request to process-csv with body:", requestBody);

      const processingResponse = await fetch("http://localhost:3000/process-csv", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: requestBody,
        signal: controller.signal,
      });

      clearTimeout(timeoutId);

      if (!processingResponse.ok) {
        const errorText = await processingResponse.text();
        console.error(`CSV Processing failed: ${processingResponse.status} - ${errorText}`);
        throw { status_code: 500, body: `Failed to process CSV: ${errorText}` };
      }

      const processingResult = await processingResponse.json();
      console.log("CSV Processing successful:", processingResult);

      if (!processingResult.success) {
        throw { status_code: 500, body: processingResult.error || "CSV processing failed" };
      }

      // Mark upload as successfully processed
      console.log("CSV processing completed successfully");
    } catch (error) {
      console.error("Error during onUploadFinish for tus upload:", error);
      console.error("Error type:", typeof error);
      console.error("Error constructor:", error?.constructor?.name);
      console.error("Error stack:", (error as any)?.stack);

      // Handle different error types
      let errorMessage = "Unknown error during CSV processing";
      if (typeof error === "object" && error !== null) {
        if ("body" in error && typeof error.body === "string") {
          errorMessage = error.body;
        } else if ("message" in error && typeof error.message === "string") {
          errorMessage = error.message;
        } else if (error instanceof Error) {
          if (error.name === "AbortError") {
            errorMessage = "CSV processing timed out. Please try again with a smaller file.";
          } else if (error.message.includes("fetch failed")) {
            errorMessage =
              "CSV processing service is not available. Please check if the processing service is running.";
          } else {
            errorMessage = error.message;
          }
        }
      }

      console.error("Final error message:", errorMessage);

      // Store error information in upload metadata
      console.error("Upload completed but processing failed. Error:", errorMessage);

      // Instead of throwing, we'll store the error in metadata
      // The client can check the status later
      console.log("Error stored in upload metadata. Client should check status endpoint.");
    }

    return {};
  },
});

export const GET = async (req: NextRequest) => {
  try {
    return await server.handleWeb(req);
  } catch (error) {
    console.error("TUS server error in GET:", error);
    return new Response("Internal Server Error", { status: 500 });
  }
};

export const POST = async (req: NextRequest) => {
  try {
    return await server.handleWeb(req);
  } catch (error) {
    console.error("TUS server error in POST:", error);
    return new Response("Internal Server Error", { status: 500 });
  }
};

export const PATCH = async (req: NextRequest) => {
  try {
    return await server.handleWeb(req);
  } catch (error) {
    console.error("TUS server error in PATCH:", error);
    return new Response("Internal Server Error", { status: 500 });
  }
};

export const DELETE = async (req: NextRequest) => {
  try {
    return await server.handleWeb(req);
  } catch (error) {
    console.error("TUS server error in DELETE:", error);
    return new Response("Internal Server Error", { status: 500 });
  }
};

export const OPTIONS = async (req: NextRequest) => {
  try {
    return await server.handleWeb(req);
  } catch (error) {
    console.error("TUS server error in OPTIONS:", error);
    return new Response("Internal Server Error", { status: 500 });
  }
};

export const HEAD = async (req: NextRequest) => {
  try {
    return await server.handleWeb(req);
  } catch (error) {
    console.error("TUS server error in HEAD:", error);
    return new Response("Internal Server Error", { status: 500 });
  }
};
