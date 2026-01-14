"use client";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { FileText, X, Upload, Trash2 } from "lucide-react";
import { useCallback, useRef, useState } from "react";
import { cn } from "@/lib/utils";
import api from "@/lib/api";

export default function AdminPage() {
  const [fileName, setFileName] = useState<string | null>(null);
  const [file, setFile] = useState<File | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [isDragging, setIsDragging] = useState(false);
  const [isSubmitting, setIsSubmitting] = useState(false);

  const handleThumbnailClick = useCallback(() => {
    fileInputRef.current?.click();
  }, []);

  const handleFileChange = useCallback(
    (event: React.ChangeEvent<HTMLInputElement>) => {
      const selectedFile = event.target.files?.[0];
      if (
        selectedFile &&
        (selectedFile.type === "text/csv" || selectedFile.name.endsWith(".csv"))
      ) {
        setFileName(selectedFile.name);
        setFile(selectedFile);
        console.log("Uploaded CSV file:", selectedFile.name);
      }
    },
    []
  );

  const handleSubmit = useCallback(async () => {
    if (!file) return;

    setIsSubmitting(true);
    try {
      const formData = new FormData();
      formData.append("csvFile", file);

      const response = await api.post("/upload-csv", formData, {
        headers: {
          "Content-Type": "multipart/form-data",
        },
      });

      console.log("Upload successful:", response.data);
      alert("CSV uploaded successfully!");
    } catch (error) {
      console.error("Upload failed:", error);
      alert("Failed to upload CSV. Please try again.");
    } finally {
      setIsSubmitting(false);
    }
  }, [file]);

  const handleRemove = useCallback(() => {
    setFileName(null);
    setFile(null);
    if (fileInputRef.current) {
      fileInputRef.current.value = "";
    }
  }, []);

  const handleDragOver = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    e.stopPropagation();
  };

  const handleDragEnter = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(true);
  };

  const handleDragLeave = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(false);
  };

  const handleDrop = useCallback((e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(false);

    const droppedFile = e.dataTransfer.files?.[0];
    if (
      droppedFile &&
      (droppedFile.type === "text/csv" || droppedFile.name.endsWith(".csv"))
    ) {
      setFileName(droppedFile.name);
      setFile(droppedFile);
      console.log("Uploaded CSV file:", droppedFile.name);
    }
  }, []);

  return (
    <div className="min-h-screen bg-linear-to-br from-background to-muted/20">
      <div className="container mx-auto px-4 py-16">
        {/* Hero Header */}
        <div className="text-center mb-12">
          <h1 className="text-4xl font-bold tracking-tight text-foreground mb-4">
            CSV File Upload
          </h1>
          <p className="text-xl text-muted-foreground max-w-2xl mx-auto">
            Upload your CSV files securely and efficiently. Our system supports
            drag-and-drop functionality for a seamless experience.
          </p>
        </div>

        {/* Centered Upload Component */}
        <div className="flex justify-center">
          <div className="w-full max-w-md space-y-6 rounded-xl border border-border bg-card p-6 shadow-sm">
            <div className="space-y-2">
              <h3 className="text-lg font-medium">CSV Upload</h3>
              <p className="text-sm text-muted-foreground">
                Supported format: CSV
              </p>
            </div>

            <Input
              type="file"
              accept=".csv,text/csv"
              className="hidden"
              ref={fileInputRef}
              onChange={handleFileChange}
            />

            {!fileName ? (
              <div
                onClick={handleThumbnailClick}
                onDragOver={handleDragOver}
                onDragEnter={handleDragEnter}
                onDragLeave={handleDragLeave}
                onDrop={handleDrop}
                className={cn(
                  "flex h-64 cursor-pointer flex-col items-center justify-center gap-4 rounded-lg border-2 border-dashed border-muted-foreground/25 bg-muted/50 transition-colors hover:bg-muted",
                  isDragging && "border-primary/50 bg-primary/5"
                )}
              >
                <div className="rounded-full bg-background p-3 shadow-sm">
                  <FileText className="h-6 w-6 text-muted-foreground" />
                </div>
                <div className="text-center">
                  <p className="text-sm font-medium">Click to select</p>
                  <p className="text-xs text-muted-foreground">
                    or drag and drop CSV file here
                  </p>
                </div>
              </div>
            ) : (
              <div className="relative">
                <div className="flex h-64 flex-col items-center justify-center gap-4 rounded-lg border-2 border-dashed border-muted-foreground/25 bg-muted/50">
                  <div className="rounded-full bg-background p-3 shadow-sm">
                    <FileText className="h-6 w-6 text-muted-foreground" />
                  </div>
                  <div className="text-center">
                    <p className="text-sm font-medium">{fileName}</p>
                    <p className="text-xs text-muted-foreground">
                      CSV file uploaded
                    </p>
                  </div>
                  <div className="flex gap-2">
                    <Button
                      size="sm"
                      variant="secondary"
                      onClick={handleThumbnailClick}
                      className="h-9 w-9 p-0"
                    >
                      <Upload className="h-4 w-4" />
                    </Button>
                    <Button
                      size="sm"
                      variant="destructive"
                      onClick={handleRemove}
                      className="h-9 w-9 p-0"
                    >
                      <Trash2 className="h-4 w-4" />
                    </Button>
                  </div>
                </div>
                <Button
                  onClick={handleSubmit}
                  disabled={isSubmitting}
                  className="w-full mt-4"
                >
                  {isSubmitting ? "Submitting..." : "Submit CSV"}
                </Button>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
