import {Component, OnInit} from '@angular/core';
import { Router, RouterLink, RouterLinkActive } from '@angular/router';
import {CommonModule} from '@angular/common';
import {FormsModule} from '@angular/forms';
import {HttpClient, HttpErrorResponse} from '@angular/common/http';
import { SessionService } from '../../session.service';

type RowRecord = Record<string, any>;

@Component({
    selector: 'app-analytics',
    standalone:true,
    imports: [CommonModule, FormsModule, RouterLink, RouterLinkActive],
    templateUrl: './analytics.page.html',
    styleUrls: ['./analytics.page.css']
})
export class AnalyticsPage implements OnInit {
    
    datasets: any[] = [];
    versions: any[] = [];
    previewA: RowRecord[] | null = null;
    previewB: RowRecord[] | null = null;
    filteredDataA: RowRecord[] = [];
    filteredDataB: RowRecord[] = [];
    /** Use '' (not null) so <select value=""> matches ngModel and the control updates reliably. */
    selectedDataset = '';
    selectedVersionA = '';
    selectedVersionB = '';

    datasetsLoadError: string | null = null;
    versionsLoadError: string | null = null;
    previewLoadErrorA: string | null = null;
    previewLoadErrorB: string | null = null;

    // Filter properties
    searchTerm = '';
    filterColumn = '';
    filterValue = '';
    columnsA: string[] = [];
    columnsB: string[] = [];
    columnsUnion: string[] = [];

    // View mode
    viewMode: 'compare' | 'diff' = 'compare';

    // Diff (best-effort by row index)
    diffAddedColumns: string[] = [];
    diffRemovedColumns: string[] = [];
    diffCommonColumns: string[] = [];
    diffRowCountCompared = 0;
    diffChangedCellsCount = 0;
    diffSamples: Array<{ rowIndex: number; changes: Array<{ column: string; a: string; b: string }> }> = [];
    diffWarning =
        'Row-by-row diff is best-effort by row index. If rows were filtered/sorted/deduped between versions, alignment can shift.';
    
    backend = 'http://127.0.0.1:8000';
    isAdmin = false;

    constructor(
        private http: HttpClient,
        private router: Router,
        private session: SessionService,
    ) {}

    async ngOnInit() {
        if (!localStorage.getItem('token')) {
            void this.router.navigate(['/']);
            return;
        }
        this.isAdmin = await this.session.isAdmin();
        this.loadDatasets();
    }

    loadDatasets() {
        this.datasetsLoadError = null;
        this.http.get<any[]>(`${this.backend}/datasets`).subscribe({
            next: (d) => {
                this.datasets = Array.isArray(d) ? d : [];
            },
            error: (e: HttpErrorResponse) => {
                this.datasets = [];
                this.datasetsLoadError =
                    typeof e.error?.detail === 'string' ? e.error.detail : e.message || `HTTP ${e.status}`;
            },
        });
    }

    onDatasetChange(datasetId: string) {
        this.versionsLoadError = null;
        this.previewLoadErrorA = null;
        this.previewLoadErrorB = null;
        this.previewA = null;
        this.previewB = null;
        this.filteredDataA = [];
        this.filteredDataB = [];
        this.selectedVersionA = '';
        this.selectedVersionB = '';
        this.searchTerm = '';
        this.filterColumn = '';
        this.filterValue = '';
        this.viewMode = 'compare';
        this.columnsA = [];
        this.columnsB = [];
        this.columnsUnion = [];
        this.resetDiff();

        if (!datasetId) {
            this.versions = [];
            return;
        }

        this.http.get<any[]>(`${this.backend}/datasets/${datasetId}/versions`).subscribe({
            next: (v) => {
                const versions = Array.isArray(v) ? v : [];
                // Try to keep versions in ascending versionNumber order if present.
                this.versions = [...versions].sort((a, b) => {
                    const av = Number(a?.versionNumber);
                    const bv = Number(b?.versionNumber);
                    if (Number.isFinite(av) && Number.isFinite(bv)) return av - bv;
                    return String(a?.id ?? '').localeCompare(String(b?.id ?? ''));
                });

                // Defaults: B = latest, A = previous if available.
                if (this.versions.length > 0) {
                    const latest = this.versions[this.versions.length - 1];
                    const previous = this.versions.length > 1 ? this.versions[this.versions.length - 2] : null;
                    this.selectedVersionB = String(latest?.id ?? '');
                    this.selectedVersionA = previous ? String(previous?.id ?? '') : '';
                    this.onVersionsChange();
                }
            },
            error: (e: HttpErrorResponse) => {
                this.versions = [];
                this.versionsLoadError =
                    typeof e.error?.detail === 'string' ? e.error.detail : e.message || `HTTP ${e.status}`;
            },
        });
    }

    onVersionsChange() {
        this.previewLoadErrorA = null;
        this.previewLoadErrorB = null;
        this.previewA = null;
        this.previewB = null;
        this.filteredDataA = [];
        this.filteredDataB = [];
        this.columnsA = [];
        this.columnsB = [];
        this.columnsUnion = [];
        this.resetDiff();

        if (!this.selectedVersionB && !this.selectedVersionA) {
            return;
        }

        if (this.selectedVersionA) this.loadPreview('A', this.selectedVersionA);
        if (this.selectedVersionB) this.loadPreview('B', this.selectedVersionB);
    }

    applyFilters() {
        this.filteredDataA = this.applyFiltersTo(this.previewA ?? [], 'A');
        this.filteredDataB = this.applyFiltersTo(this.previewB ?? [], 'B');
        this.computeDiff();
    }

    clearFilters() {
        this.searchTerm = '';
        this.filterColumn = '';
        this.filterValue = '';
        this.filteredDataA = this.previewA ?? [];
        this.filteredDataB = this.previewB ?? [];
        this.computeDiff();
    }

    onSignOut() {
        localStorage.removeItem('token');
        this.session.clear();
        void this.router.navigate(['/']);
    }

    private loadPreview(which: 'A' | 'B', versionId: string) {
        if (!versionId) return;
        const url = `${this.backend}/versions/${versionId}/records`;
        this.http.get<RowRecord[]>(url).subscribe({
            next: (data: RowRecord[]) => {
                const rows = Array.isArray(data) ? data : [];
                if (which === 'A') {
                    this.previewA = rows;
                    this.columnsA = rows.length > 0 ? Object.keys(rows[0] ?? {}) : [];
                    this.filteredDataA = this.applyFiltersTo(rows, 'A');
                } else {
                    this.previewB = rows;
                    this.columnsB = rows.length > 0 ? Object.keys(rows[0] ?? {}) : [];
                    this.filteredDataB = this.applyFiltersTo(rows, 'B');
                }
                this.columnsUnion = this.unionColumns(this.columnsA, this.columnsB);
                this.computeDiff();
            },
            error: (e: HttpErrorResponse) => {
                const msg = typeof e.error?.detail === 'string' ? e.error.detail : e.message || `HTTP ${e.status}`;
                if (which === 'A') this.previewLoadErrorA = msg;
                else this.previewLoadErrorB = msg;
            },
        });
    }

    private applyFiltersTo(rows: RowRecord[], _which: 'A' | 'B'): RowRecord[] {
        let data = [...(rows ?? [])];

        if (this.searchTerm) {
            const term = this.searchTerm.toLowerCase();
            data = data.filter((row) =>
                Object.values(row ?? {}).some((val) => String(val ?? '').toLowerCase().includes(term)),
            );
        }

        if (this.filterColumn && this.filterValue) {
            const val = this.filterValue.toLowerCase();
            data = data.filter((row) =>
                String((row ?? {})[this.filterColumn] ?? '')
                    .toLowerCase()
                    .includes(val),
            );
        }

        return data;
    }

    private unionColumns(a: string[], b: string[]) {
        const s = new Set<string>();
        for (const c of a ?? []) s.add(c);
        for (const c of b ?? []) s.add(c);
        return [...s];
    }

    private resetDiff() {
        this.diffAddedColumns = [];
        this.diffRemovedColumns = [];
        this.diffCommonColumns = [];
        this.diffRowCountCompared = 0;
        this.diffChangedCellsCount = 0;
        this.diffSamples = [];
    }

    private computeDiff() {
        const colsA = new Set(this.columnsA ?? []);
        const colsB = new Set(this.columnsB ?? []);
        this.diffAddedColumns = [...colsB].filter((c) => !colsA.has(c)).sort();
        this.diffRemovedColumns = [...colsA].filter((c) => !colsB.has(c)).sort();
        this.diffCommonColumns = [...colsA].filter((c) => colsB.has(c)).sort();

        const a = this.filteredDataA ?? [];
        const b = this.filteredDataB ?? [];
        const n = Math.min(a.length, b.length);
        this.diffRowCountCompared = n;

        let changedCells = 0;
        const samples: Array<{ rowIndex: number; changes: Array<{ column: string; a: string; b: string }> }> = [];
        const maxSampleRows = 25;
        const maxSampleColsPerRow = 12;

        for (let i = 0; i < n; i++) {
            const ra = a[i] ?? {};
            const rb = b[i] ?? {};
            let rowChanges: Array<{ column: string; a: string; b: string }> = [];
            for (const c of this.diffCommonColumns) {
                const av = String(ra[c] ?? '');
                const bv = String(rb[c] ?? '');
                if (av !== bv) {
                    changedCells++;
                    if (rowChanges.length < maxSampleColsPerRow) rowChanges.push({ column: c, a: av, b: bv });
                }
            }
            if (rowChanges.length > 0 && samples.length < maxSampleRows) {
                samples.push({ rowIndex: i, changes: rowChanges });
            }
        }

        this.diffChangedCellsCount = changedCells;
        this.diffSamples = samples;
    }
}