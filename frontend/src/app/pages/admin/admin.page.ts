import { CommonModule } from '@angular/common';
import { Component, computed, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Router, RouterLink, RouterLinkActive } from '@angular/router';

import { AdminApiService, LogRow, UserRow } from './admin.service';
import { SessionService } from '../../session.service';

@Component({
  selector: 'app-admin-page',
  standalone: true,
  imports: [CommonModule, FormsModule, RouterLink, RouterLinkActive],
  templateUrl: './admin.page.html',
  styleUrl: './admin.page.css',
  providers: [AdminApiService]
})
export class AdminPage {
  protected readonly me = signal<{ username: string; role: string; isAdmin: boolean } | null>(null);

  protected readonly users = signal<UserRow[]>([]);
  protected readonly usersBusy = signal(false);
  protected readonly usersError = signal<string | null>(null);
  protected readonly createUsername = signal('');
  protected readonly createPassword = signal('');
  protected readonly createRole = signal<'user' | 'admin'>('user');
  protected readonly createBusy = signal(false);

  protected readonly datasets = signal<Array<{ id: string; name: string; createdAt: string; ownerId?: string | null }>>(
    []
  );
  protected readonly datasetsBusy = signal(false);
  protected readonly datasetsError = signal<string | null>(null);
  protected readonly datasetOwnerFilter = signal<string>('');

  protected readonly logs = signal<LogRow[]>([]);
  protected readonly logsBusy = signal(false);
  protected readonly logsError = signal<string | null>(null);
  protected readonly logsOwnerFilter = signal<string>('');

  protected readonly sortedUsers = computed(() => {
    const list = [...(this.users() ?? [])];
    list.sort((a, b) => String(a.username).localeCompare(String(b.username)));
    return list;
  });

  constructor(
    private readonly api: AdminApiService,
    private readonly session: SessionService,
    private readonly router: Router
  ) {
    if (!localStorage.getItem('token')) {
      void this.router.navigate(['/']);
      return;
    }
    void this.bootstrap();
  }

  private async bootstrap() {
    try {
      this.me.set(await this.session.me());
    } catch {
      // guard should already prevent entry, but be defensive
      void this.router.navigate(['/']);
      return;
    }
    await Promise.all([this.refreshUsers(), this.refreshDatasets(), this.refreshLogs()]);
  }

  protected async refreshUsers() {
    this.usersError.set(null);
    this.usersBusy.set(true);
    try {
      this.users.set(await this.api.listUsers());
    } catch (e) {
      this.users.set([]);
      this.usersError.set(this.api.errToMessage(e));
    } finally {
      this.usersBusy.set(false);
    }
  }

  protected async refreshDatasets() {
    this.datasetsError.set(null);
    this.datasetsBusy.set(true);
    try {
      const owner = this.datasetOwnerFilter().trim();
      const all = await this.api.listDatasets();
      const filtered = owner ? (all ?? []).filter((d) => String(d.ownerId ?? '') === owner) : (all ?? []);
      filtered.sort((a, b) => String(a.createdAt).localeCompare(String(b.createdAt)));
      this.datasets.set(filtered);
    } catch (e) {
      this.datasets.set([]);
      this.datasetsError.set(this.api.errToMessage(e));
    } finally {
      this.datasetsBusy.set(false);
    }
  }

  protected async deleteDatasetRow(d: { id: string; name: string; ownerId?: string | null }) {
    this.datasetsError.set(null);
    const label = `${d.name} (${d.id})`;
    const owner = d.ownerId ? ` owned by ${d.ownerId}` : '';
    const ok = window.confirm(`Delete dataset ${label}${owner}? This permanently removes all versions and bytes.`);
    if (!ok) return;

    this.datasetsBusy.set(true);
    try {
      await this.api.deleteDataset(d.id);
      this.datasets.set(this.datasets().filter((x) => x.id !== d.id));
    } catch (e) {
      this.datasetsError.set(this.api.errToMessage(e));
    } finally {
      this.datasetsBusy.set(false);
    }
  }

  protected async changeRole(u: UserRow, role: 'admin' | 'user') {
    this.usersError.set(null);
    const username = u.username;
    try {
      const updated = await this.api.setRole(username, role);
      this.users.set(this.users().map((x) => (x.username === username ? updated : x)));
      // If you changed your own role, refresh /me so nav updates.
      const me = this.me();
      if (me && me.username === username) {
        this.session.clear();
        this.me.set(await this.session.me(true));
      }
    } catch (e) {
      this.usersError.set(this.api.errToMessage(e));
    }
  }

  protected async createUser() {
    this.usersError.set(null);
    const username = this.createUsername().trim();
    const password = this.createPassword();
    const role = this.createRole();
    if (!username) {
      this.usersError.set('Username is required.');
      return;
    }
    if (!password) {
      this.usersError.set('Password is required.');
      return;
    }

    this.createBusy.set(true);
    try {
      const created = await this.api.createUser({ username, password, role });
      this.users.set([...this.users(), created]);
      this.createUsername.set('');
      this.createPassword.set('');
      this.createRole.set('user');
    } catch (e) {
      this.usersError.set(this.api.errToMessage(e));
    } finally {
      this.createBusy.set(false);
    }
  }

  protected async refreshLogs() {
    this.logsError.set(null);
    this.logsBusy.set(true);
    try {
      const owner = this.logsOwnerFilter().trim();
      this.logs.set(await this.api.listLogs(200, owner || undefined));
    } catch (e) {
      this.logs.set([]);
      this.logsError.set(this.api.errToMessage(e));
    } finally {
      this.logsBusy.set(false);
    }
  }

  protected onSignOut() {
    localStorage.removeItem('token');
    this.session.clear();
    void this.router.navigate(['/']);
  }
}

