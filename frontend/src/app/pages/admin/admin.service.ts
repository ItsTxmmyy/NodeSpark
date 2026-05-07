import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { firstValueFrom } from 'rxjs';

export type UserRow = { username: string; role?: 'admin' | 'user'; createdAt?: string };
export type LogRow = {
  ownerId: string;
  timestamp: string;
  event: string;
  datasetId?: string;
  versionId?: string;
  inputVersionId?: string;
  outputVersionId?: string;
  sourceVersionId?: string;
  newVersionId?: string;
  name?: string;
  format?: string;
};

@Injectable()
export class AdminApiService {
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

  async listUsers(): Promise<UserRow[]> {
    const res = await firstValueFrom(this.http.get<{ users: UserRow[] }>(`${this.baseUrl}/admin/users`));
    return res?.users ?? [];
  }

  async setRole(username: string, role: 'admin' | 'user'): Promise<UserRow> {
    const res = await firstValueFrom(
      this.http.patch<{ user: UserRow }>(`${this.baseUrl}/admin/users/${encodeURIComponent(username)}/role`, { role })
    );
    return res.user;
  }

  async createUser(body: { username: string; password: string; role: 'admin' | 'user' }): Promise<UserRow> {
    const res = await firstValueFrom(
      this.http.post<{ user: UserRow }>(`${this.baseUrl}/admin/users`, body)
    );
    return res.user;
  }

  async listDatasets(): Promise<Array<{ id: string; name: string; createdAt: string; ownerId?: string | null }>> {
    return await firstValueFrom(this.http.get<any[]>(`${this.baseUrl}/datasets`));
  }

  async deleteDataset(datasetId: string): Promise<any> {
    return await firstValueFrom(
      this.http.delete<any>(`${this.baseUrl}/admin/datasets/${encodeURIComponent(datasetId)}`)
    );
  }

  async listLogs(limit = 100, ownerId?: string): Promise<LogRow[]> {
    const params = new URLSearchParams({ limit: String(limit) });
    if (ownerId) params.set('ownerId', ownerId);
    const url = `${this.baseUrl}/logs?${params.toString()}`;
    return await firstValueFrom(this.http.get<LogRow[]>(url));
  }
}

