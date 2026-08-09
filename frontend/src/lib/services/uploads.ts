import { httpClient } from "@/lib/api";
import type { HttpClient } from "@/lib/http-client";
import type { UploadJobStatus } from "@/types";

export interface UploadJobCreated {
  job_id: string;
}

export function createUploadsService(client: HttpClient) {
  return {
    createJob: (formData: FormData) => client.postFormData<UploadJobCreated>("/uploads", formData),
    job: (id: string) => client.get<UploadJobStatus>(`/uploads/${id}`),
  };
}

export const uploadsService = createUploadsService(httpClient);
