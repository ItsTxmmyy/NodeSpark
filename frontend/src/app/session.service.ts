import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { firstValueFrom } from 'rxjs';

export type Me = { username: string; role: 'admin' | 'user'; isAdmin: boolean };

@Injectable({ providedIn: 'root' })
export class SessionService {
  // Keep aligned with other services for local dev.
  readonly baseUrl = 'http://127.0.0.1:8000';

  private cachedMe: Me | null = null;
  private cachedMeToken: string | null = null;

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

  clear() {
    this.cachedMe = null;
    this.cachedMeToken = null;
  }

  async me(force = false): Promise<Me> {
    const token = localStorage.getItem('token');
    if (!token) throw new Error('Not signed in');
    if (!force && this.cachedMe && this.cachedMeToken === token) return this.cachedMe;
    const res = await firstValueFrom(this.http.get<Me>(`${this.baseUrl}/me`));
    this.cachedMe = res;
    this.cachedMeToken = token;
    return res;
  }

  async isAdmin(): Promise<boolean> {
    try {
      const me = await this.me();
      return !!me?.isAdmin;
    } catch {
      return false;
    }
  }
}

