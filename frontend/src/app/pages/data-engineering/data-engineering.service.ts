import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { firstValueFrom } from 'rxjs';

type DatasetFormat = 'csv' | 'json';

@Injectable()
export class DataEngineeringApiService {
  // For local dev. Later this can move to environment.ts or a proxy config.
  readonly baseUrl = 'http://127.0.0.1:8000';

  constructor(private readonly http: HttpClient) {}

  errToMessage(e: unknown): string {
    if (e instanceof HttpErrorResponse) {
      const d = e.error;
      if (d && typeof d === 'object' && 'detail' in d) return String((d as { detail: unknown }).detail);
      return e.message || `HTTP ${e.status}`;
    }
    if (typeof e === 'object' && e && 'message' in e) return String((e as { message: unknown }).message);
    return 'Request failed.';
  }

  async getTransformations(): Promise<{ transformations: Array<{ type: string; parameters: any }> }> {
    return await firstValueFrom(this.http.get<any>(`${this.baseUrl}/transformations`));
  }

  async listDatasets(): Promise<Array<{ id: string; name: string; createdAt: string; ownerId?: string | null }>> {
    return await firstValueFrom(this.http.get<any[]>(`${this.baseUrl}/datasets`));
  }

  async createDataset(name: string, format: DatasetFormat, file: File) {
    const fd = new FormData();
    fd.append('file', file);
    const url = `${this.baseUrl}/datasets?name=${encodeURIComponent(name)}&format=${encodeURIComponent(format)}`;
    return await firstValueFrom(this.http.post<any>(url, fd));
  }

  async listVersions(datasetId: string) {
    return await firstValueFrom(this.http.get<any[]>(`${this.baseUrl}/datasets/${datasetId}/versions`));
  }

  async downloadVersionBlob(versionId: string): Promise<Blob> {
    return await firstValueFrom(
      this.http.get(`${this.baseUrl}/versions/${versionId}/download`, { responseType: 'blob' })
    );
  }

  async applyPipeline(body: {
    inputVersionId: string;
    steps: Array<{ type: string; parameters: Record<string, unknown> }>;
    outputFormat?: DatasetFormat;
  }) {
    return await firstValueFrom(this.http.post<any>(`${this.baseUrl}/pipelines/apply`, body));
  }

  async aiSuggest(body: {
    inputVersionId: string;
    prompt: string;
    sampleSize?: number;
  }): Promise<{
    steps: Array<{ type: string; parameters: Record<string, unknown> }>;
    explanation: string;
    assumptions: string[];
    needsClarification: boolean;
    clarificationQuestion: string | null;
  }> {
    return await firstValueFrom(this.http.post<any>(`${this.baseUrl}/ai/suggest`, body));
  }

  async revertVersion(versionId: string) {
    return await firstValueFrom(this.http.post<any>(`${this.baseUrl}/versions/${versionId}/revert`, {}));
  }

  async renameVersion(versionId: string, versionName: string) {
    return await firstValueFrom(
      this.http.patch<any>(`${this.baseUrl}/versions/${versionId}/name`, { versionName })
    );
  }
}

