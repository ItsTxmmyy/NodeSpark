import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { firstValueFrom } from 'rxjs';

type DatasetFormat = 'csv' | 'json';

@Injectable()
export class DataEngineeringApiService {
  // For local dev. Later this can move to environment.ts or a proxy config.
  readonly baseUrl = 'http://127.0.0.1:8000';

  constructor(private readonly http: HttpClient) {}

  errToMessage(e: unknown): string {
    if (typeof e === 'object' && e && 'message' in e) return String((e as any).message);
    return 'Request failed.';
  }

  async getTransformations(): Promise<{ transformations: Array<{ type: string; parameters: any }> }> {
    return await firstValueFrom(this.http.get<any>(`${this.baseUrl}/transformations`));
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

  async applyPipeline(body: {
    inputVersionId: string;
    steps: Array<{ type: string; parameters: Record<string, unknown> }>;
    outputFormat?: DatasetFormat;
  }) {
    return await firstValueFrom(this.http.post<any>(`${this.baseUrl}/pipelines/apply`, body));
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

