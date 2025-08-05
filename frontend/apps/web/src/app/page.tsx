"use client";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useState } from "react";
import * as tus from "tus-js-client";

export default function Home() {
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [uploadProgress, setUploadProgress] = useState<number>(0);
  const [uploadStatus, setUploadStatus] = useState<string>("");

  const resetUpload = () => {
    setSelectedFile(null);
    setUploadProgress(0);
    setUploadStatus("");
  };

  const checkProcessingStatus = async (uploadUrl: string) => {
    try {
      // Extract upload ID from the URL
      const parts = uploadUrl.split("/");
      const uploadId = parts[parts.length - 1];
      if (!uploadId) {
        setUploadStatus("Error: Could not determine upload ID");
        return;
      }

      console.log("Checking processing status for upload:", uploadId);

      // Check status after a short delay to allow processing to complete
      setTimeout(async () => {
        try {
          const response = await fetch(`/api/upload-status/${uploadId}`);
          const statusData = await response.json();

          console.log("Processing status:", statusData);

          if (statusData.status === "error") {
            setUploadStatus(`Error: ${statusData.error}`);
            setUploadProgress(0);
          } else if (statusData.status === "success") {
            setUploadStatus("Upload and CSV processing completed successfully!");
            setUploadProgress(100);
          } else {
            setUploadStatus("Upload completed, but processing status is unknown");
            setUploadProgress(100);
          }
        } catch (error) {
          console.error("Error checking processing status:", error);
          setUploadStatus("Upload completed, but could not verify processing status");
          setUploadProgress(100);
        }
      }, 2000); // Wait 2 seconds before checking
    } catch (error) {
      console.error("Error in checkProcessingStatus:", error);
      setUploadStatus("Error checking processing status");
    }
  };

  const handleUpload = async (event: React.ChangeEvent<HTMLInputElement>) => {
    if (!event.target.files || event.target.files.length === 0) {
      alert("Please select a file first.");
      return;
    }

    const file = event.target.files[0];
    setSelectedFile(file);
    setUploadProgress(0);
    setUploadStatus("Starting upload...");

    const uploadUrl = `${process.env.NEXT_PUBLIC_SERVER_URL}/api/upload/`;

    const upload = new tus.Upload(file, {
      endpoint: uploadUrl,
      retryDelays: [0, 3000, 5000, 10000],
      metadata: {
        filename: file.name,
        filetype: file.type,
      },
      headers: {}, // Add any custom headers if needed
      onError: function (error) {
        console.error("Upload failed:", error);

        // Handle different types of errors
        let errorMessage = "Upload failed";

        if (error.message) {
          errorMessage = error.message;
        } else if (typeof error === "object" && error !== null && "originalRequest" in error) {
          const originalRequest = (error as any).originalRequest;
          if (originalRequest && typeof originalRequest.getStatus === "function") {
            const status = originalRequest.getStatus();
            if (status === 500) {
              errorMessage = "CSV processing failed. Please check your file format and try again.";
            } else if (status === 400) {
              errorMessage = "Invalid file type. Only CSV files are allowed.";
            } else if (status === 413) {
              errorMessage = "File too large. Maximum size is 50MB.";
            } else {
              errorMessage = `Upload failed with status ${status}`;
            }
          }
        }

        setUploadStatus(`Error: ${errorMessage}`);
        setUploadProgress(0);
      },
      onProgress: function (bytesUploaded, bytesTotal) {
        const percentage = ((bytesUploaded / bytesTotal) * 100).toFixed(2);
        setUploadProgress(parseFloat(percentage));
        setUploadStatus(`Uploading: ${percentage}%`);
      },
      onSuccess: function (payload) {
        console.log("Upload completed:", payload);
        console.log("Upload URL:", upload.url);

        // Check the processing status after upload completes
        if (upload.url) {
          checkProcessingStatus(upload.url);
        } else {
          setUploadStatus("Upload completed, but could not determine upload URL");
        }
      },
      onChunkComplete: function (chunkSize, bytesAccepted, bytesTotal) {
        console.log(`Chunk complete: ${chunkSize} bytes accepted, ${bytesAccepted}/${bytesTotal} total`);
      },
    });

    // Check if there are any previous uploads to resume
    upload.findPreviousUploads().then(function (previousUploads) {
      // Found previous uploads so we can resume them
      if (previousUploads.length) {
        upload.resumeFromPreviousUpload(previousUploads[0]);
        setUploadStatus("Resuming upload...");
      }

      // Start the upload
      upload.start();
    });
  };

  return (
    <div className="container mx-auto max-w-3xl px-4 py-4">
      <div className="grid w-full max-w-sm items-center gap-3 mb-4">
        <Label htmlFor="file">File</Label>
        <Input id="file" type="file" accept="text/csv" onChange={handleUpload} />
      </div>

      {selectedFile && (
        <>
          <p className="mb-2">
            Selected file: <strong>{selectedFile.name}</strong> ({selectedFile.type}, {selectedFile.size}{" "}
            bytes)
          </p>
          {uploadStatus && (
            <p
              className={`mb-2 p-2 rounded ${
                uploadStatus.startsWith("Error:")
                  ? "bg-red-100 text-red-800 border border-red-200"
                  : uploadStatus.includes("successfully")
                  ? "bg-green-100 text-green-800 border border-green-200"
                  : "bg-blue-100 text-blue-800 border border-blue-200"
              }`}
            >
              Status: {uploadStatus}
            </p>
          )}
          {uploadProgress > 0 && (
            <div className="w-full bg-gray-200 rounded-full h-2.5 dark:bg-gray-700">
              <div className="bg-blue-600 h-2.5 rounded-full" style={{ width: `${uploadProgress}%` }}></div>
            </div>
          )}

          {(uploadStatus.startsWith("Error:") || uploadStatus.includes("successfully")) && (
            <Button onClick={resetUpload}>Upload Another File</Button>
          )}
        </>
      )}
    </div>
  );
}
