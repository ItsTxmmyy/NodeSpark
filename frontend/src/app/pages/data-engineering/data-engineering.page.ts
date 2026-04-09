import { CommonModule } from '@angular/common';
import { Component, computed, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';

import { DataEngineeringApiService } from './data-engineering.service';

type DatasetFormat = 'csv' | 'json';

@Component({
  selector: 'app-data-engineering-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
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

  protected readonly versions = signal<
    Array<{
      id: string;
      datasetId: string;
      versionNumber: number;
      format: DatasetFormat;
      createdAt: string;
      createdFromVersionId?: string;
      recordCount?: number;
      transformation?: { type: string; parameters: Record<string, unknown>; timestamp: string };
    }>
  >([]);

  protected readonly busy = signal(false);
  protected readonly error = signal<string | null>(null);

  protected readonly canApply = computed(() => {
    return !!this.currentVersionId() && !this.busy();
  });

  constructor(private readonly api: DataEngineeringApiService) {
    this.refreshTransformations().catch(() => {});
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
  }
}

