import { CommonModule } from '@angular/common';
import { Component, computed, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Router, RouterLink, RouterLinkActive } from '@angular/router';

import { DataEngineeringApiService } from './data-engineering.service';

type DatasetFormat = 'csv' | 'json';

type DatasetRow = { id: string; name: string; createdAt: string; ownerId?: string | null };

@Component({
  selector: 'app-data-engineering-page',
  standalone: true,
  imports: [CommonModule, FormsModule, RouterLink, RouterLinkActive],
  templateUrl: './data-engineering.page.html',
  styleUrl: './data-engineering.page.css',
  providers: [DataEngineeringApiService]
})
export class DataEngineeringPage {
  protected readonly file = signal<File | null>(null);
  protected readonly uploadName = signal('sample');
  protected readonly uploadFormat = signal<DatasetFormat>('csv');

  protected readonly datasetId = signal<string | null>(null);
  protected readonly currentVersionId = signal<string | null>(null);

  protected readonly transformations = signal<
    Array<{ type: string; parameters: Record<string, unknown> }>
  >([]);

  protected readonly selectedTransformationType = signal<string>('deduplicate');
  protected readonly paramsJson = signal<string>('{}');
  protected readonly outputFormat = signal<DatasetFormat | ''>('');
  protected readonly renameVersionName = signal('');

  // AI assistant
  protected readonly aiPrompt = signal('');
  protected readonly aiBusy = signal(false);
  protected readonly aiError = signal<string | null>(null);
  protected readonly aiResult = signal<{
    steps: Array<{ type: string; parameters: Record<string, unknown> }>;
    explanation: string;
    assumptions: string[];
    needsClarification: boolean;
    clarificationQuestion: string | null;
  } | null>(null);

  protected readonly datasets = signal<DatasetRow[]>([]);

  protected readonly versions = signal<
    Array<{
      id: string;
      datasetId: string;
      versionNumber: number;
      format: DatasetFormat;
      createdAt: string;
      versionName?: string;
      createdFromVersionId?: string;
      recordCount?: number;
      transformation?: { type: string; parameters: Record<string, unknown>; timestamp: string };
    }>
  >([]);
  protected readonly versionsNewestFirst = computed(() =>
    [...this.versions()].sort((a, b) => b.versionNumber - a.versionNumber)
  );

  protected readonly busy = signal(false);
  protected readonly error = signal<string | null>(null);

  protected readonly canApply = computed(() => {
    return !!this.currentVersionId() && !this.busy();
  });
  protected readonly selectedVersion = computed(() =>
    this.versions().find((v) => v.id === this.currentVersionId()) ?? null
  );
  protected readonly downloadBusy = signal(false);

  constructor(private readonly api: DataEngineeringApiService, private readonly router: Router) {
    if (!localStorage.getItem('token')) {
      void this.router.navigate(['/']);
      return;
    }
    void this.bootstrapData();
  }

  private async bootstrapData() {
    try {
      await Promise.all([this.refreshTransformations(), this.loadDatasetsList()]);
      const saved = this.datasets();
      if (saved.length === 1) {
        await this.onOpenExistingDataset(saved[0].id);
      }
    } catch {
      /* errors surfaced via getTransformations / list if needed */
    }
  }

  protected async loadDatasetsList() {
    try {
      const rows = await this.api.listDatasets();
      this.datasets.set(rows ?? []);
    } catch {
      this.datasets.set([]);
    }
  }

  protected async onOpenExistingDataset(datasetId: string) {
    if (!datasetId) {
      this.datasetId.set(null);
      this.currentVersionId.set(null);
      this.versions.set([]);
      return;
    }
    this.error.set(null);
    this.busy.set(true);
    try {
      this.datasetId.set(datasetId);
      await this.refreshVersions();
      const list = this.versions();
      const latest = list.reduce(
        (best, v) => (!best || v.versionNumber > best.versionNumber ? v : best),
        null as (typeof list)[0] | null
      );
      this.currentVersionId.set(latest?.id ?? null);
      const cur = list.find((v) => v.id === this.currentVersionId());
      this.renameVersionName.set(cur?.versionName ?? '');
    } catch (e) {
      this.error.set(this.api.errToMessage(e));
      this.datasetId.set(null);
      this.versions.set([]);
    } finally {
      this.busy.set(false);
    }
  }

  protected onSignOut() {
    localStorage.removeItem('token');
    this.router.navigate(['/']);
  }

  protected onFilePicked(e: Event) {
    const input = e.target as HTMLInputElement;
    this.file.set(input.files?.item(0) ?? null);
  }

  protected async refreshTransformations() {
    const caps = await this.api.getTransformations();
    this.transformations.set(caps.transformations ?? []);
    const first = (caps.transformations?.[0]?.type ?? 'deduplicate') as string;
    this.selectedTransformationType.set(first);
  }

  protected async upload() {
    this.error.set(null);
    const f = this.file();
    if (!f) {
      this.error.set('Pick a CSV or JSON file first.');
      return;
    }
    this.busy.set(true);
    try {
      const res = await this.api.createDataset(this.uploadName(), this.uploadFormat(), f);
      this.datasetId.set(res.dataset.id);
      this.currentVersionId.set(res.version.id);
      await this.refreshVersions();
      await this.loadDatasetsList();
    } catch (e) {
      this.error.set(this.api.errToMessage(e));
    } finally {
      this.busy.set(false);
    }
  }

  protected async refreshVersions() {
    const did = this.datasetId();
    if (!did) return;
    const list = await this.api.listVersions(did);
    this.versions.set(list);
    const current = list.find((v) => v.id === this.currentVersionId());
    this.renameVersionName.set(current?.versionName ?? '');
  }

  protected async applyOne() {
    this.error.set(null);
    const inputVersionId = this.currentVersionId();
    if (!inputVersionId) return;

    let params: Record<string, unknown> = {};
    try {
      params = JSON.parse(this.paramsJson() || '{}');
    } catch {
      this.error.set('Parameters must be valid JSON.');
      return;
    }

    this.busy.set(true);
    try {
      const res = await this.api.applyPipeline({
        inputVersionId,
        steps: [{ type: this.selectedTransformationType(), parameters: params }],
        outputFormat: this.outputFormat() || undefined
      });

      // Each step creates a version; for a single step, there is 1 created id.
      const newId = res.outputVersionId;
      this.currentVersionId.set(newId);
      await this.refreshVersions();
    } catch (e) {
      this.error.set(this.api.errToMessage(e));
    } finally {
      this.busy.set(false);
    }
  }

  protected setCurrentVersion(versionId: string) {
    this.currentVersionId.set(versionId);
    const selected = this.versions().find((v) => v.id === versionId);
    this.renameVersionName.set(selected?.versionName ?? '');
  }

  protected async revertSelectedVersion() {
    this.error.set(null);
    const versionId = this.currentVersionId();
    if (!versionId) {
      this.error.set('Select a version to revert first.');
      return;
    }

    const selected = this.versions().find((v) => v.id === versionId);
    const versionLabel = selected ? `v${selected.versionNumber}` : versionId;
    const confirmed = window.confirm(
      `Revert to ${versionLabel}? This will create a new latest version and keep full history.`
    );
    if (!confirmed) return;

    this.busy.set(true);
    try {
      const created = await this.api.revertVersion(versionId);
      this.currentVersionId.set(created.id);
      await this.refreshVersions();
    } catch (e) {
      this.error.set(this.api.errToMessage(e));
    } finally {
      this.busy.set(false);
    }
  }

  protected async renameSelectedVersion() {
    this.error.set(null);
    const versionId = this.currentVersionId();
    if (!versionId) {
      this.error.set('Select a version to rename first.');
      return;
    }

    this.busy.set(true);
    try {
      const updated = await this.api.renameVersion(versionId, this.renameVersionName());
      this.currentVersionId.set(updated.id);
      this.renameVersionName.set(updated.versionName ?? '');
      await this.refreshVersions();
    } catch (e) {
      this.error.set(this.api.errToMessage(e));
    } finally {
      this.busy.set(false);
    }
  }

  protected async runAiSuggest() {
    this.aiError.set(null);
    this.aiResult.set(null);

    const inputVersionId = this.currentVersionId();
    if (!inputVersionId) {
      this.aiError.set('Select a version first.');
      return;
    }
    const prompt = this.aiPrompt().trim();
    if (!prompt) {
      this.aiError.set('Describe what you want to do (e.g. "remove empty rows and deduplicate by email").');
      return;
    }

    this.aiBusy.set(true);
    try {
      const res = await this.api.aiSuggest({ inputVersionId, prompt, sampleSize: 20 });
      this.aiResult.set(res);
    } catch (e) {
      this.aiError.set(this.api.errToMessage(e));
    } finally {
      this.aiBusy.set(false);
    }
  }

  protected useFirstAiStep() {
    const r = this.aiResult();
    const first = r?.steps?.[0];
    if (!first) return;
    this.selectedTransformationType.set(first.type);
    this.paramsJson.set(JSON.stringify(first.parameters ?? {}, null, 2));
  }

  protected async downloadSelectedVersion() {
    this.error.set(null);
    const v = this.selectedVersion();
    if (!v) {
      this.error.set('Select a version to download first.');
      return;
    }

    this.downloadBusy.set(true);
    try {
      const blob = await this.api.downloadVersionBlob(v.id);
      const ext = v.format === 'json' ? 'json' : 'csv';
      const base = (v.versionName || `v${v.versionNumber}`).replaceAll('/', '-');
      const filename = `${base}.${ext}`;

      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
    } catch (e) {
      this.error.set(this.api.errToMessage(e));
    } finally {
      this.downloadBusy.set(false);
    }
  }
}

