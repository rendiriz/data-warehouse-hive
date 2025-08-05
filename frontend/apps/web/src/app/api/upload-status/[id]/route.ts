import { NextRequest, NextResponse } from "next/server";
import { S3Store } from "@tus/s3-store";

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

export async function GET(request: NextRequest, { params }: { params: Promise<{ id: string }> }) {
  try {
    const { id: uploadId } = await params;
    console.log("Checking status for upload:", uploadId);

    // Get the upload from the datastore
    const upload = await s3Store.getUpload(uploadId);

    if (!upload) {
      return NextResponse.json({ error: "Upload not found" }, { status: 404 });
    }

    // Check if there's an error in the metadata
    const metadata = upload.metadata || {};

    if (metadata.processing_error) {
      return NextResponse.json({
        status: "error",
        error: metadata.processing_error,
        uploadId: uploadId,
      });
    }

    if (metadata.processing_status === "success") {
      return NextResponse.json({
        status: "success",
        uploadId: uploadId,
        completedAt: metadata.processing_completed_at,
      });
    }

    // If no processing status is found, assume it's still processing or failed
    return NextResponse.json({
      status: "unknown",
      uploadId: uploadId,
      message: "Processing status unknown",
    });
  } catch (error) {
    console.error("Error checking upload status:", error);
    return NextResponse.json({ error: "Failed to check upload status" }, { status: 500 });
  }
}
