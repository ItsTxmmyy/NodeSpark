import {Component, ElementRef, ViewChild, AfterViewInit} from '@angular/core';
import {CommonModule} from '@angular/common';
import {FormsModule} from '@angular/forms';
import {HttpClient} from '@angular/common/http';
import {DomSanitizer, SafeResourceUrl} from '@angular/platform-browser';

@Component({
    selector: 'app-analytics',
    standalone:true,
    imports: [CommonModule, FormsModule],
    templateUrl: './analytics.page.html',
    styleUrls: ['./analytics.page.css']
})
export class AnalyticsPage implements AfterViewInit {
    @ViewChild('powerBiContainer') powerBiContainer!: ElementRef;
    
    datasets: any [] = [];
    versions: any[] = [];
    preview: any = null;
    filteredData: any[] = [];
    embedConfig: any = null;

    selectedDataset: string | null = null;
    selectedVersion:string | null = null;

    // Filter properties
    searchTerm = '';
    filterColumn = '';
    filterValue = '';
    columns: string[] = [];

    // View mode
    viewMode: 'table' | 'powerbi' = 'table';
    
    // Power BI
    powerBiUrl = '';
    powerBiEmbedUrl = '';
    manualPowerBiUrl = '';
    safePowerBiUrl: SafeResourceUrl | null = null;
    
    backend = 'http://127.0.0.1:8000';

    constructor(
        private http: HttpClient,
        private sanitizer: DomSanitizer
    ) {}
    
    ngAfterViewInit() {
        // Container ready
    }

    loadDatasets() {
        this.http.get<any[]>(`${this.backend}/datasets/`).subscribe(d => this.datasets = d);
    }
    
    onDatasetChange(datasetId: string) {
        this.selectedDataset = datasetId;
        this.preview = null;
        this.filteredData = [];
        this.selectedVersion = null;
        this.embedConfig = null;
        this.powerBiEmbedUrl = '';
        this.safePowerBiUrl = null;
        this.searchTerm = '';
        this.filterColumn = '';
        this.filterValue = '';
        this.viewMode = 'table';

        this.http.get<any[]>(`${this.backend}/datasets/${datasetId}/versions`).subscribe((v) => (this.versions = v));
    }

    onVersionChange(versionId: string) {
        this.selectedVersion = versionId;
        this.powerBiUrl = `${this.backend}/powerbi/${versionId}`;

        // Get embed config from backend
        this.http.get<any>(`${this.backend}/powerbi/embed/${versionId}`).subscribe(config => {
            if (config && config.embedUrl) {
                this.powerBiEmbedUrl = config.embedUrl;
                this.safePowerBiUrl = this.sanitizer.bypassSecurityTrustResourceUrl(config.embedUrl);
            }
        });

        this.http.get<any[]>(this.powerBiUrl).subscribe((data: any[]) => {
            this.preview = data;
            this.filteredData = data;
            if (data.length > 0) {
                this.columns = Object.keys(data[0]);
            }
        });
    }

    setPowerBiUrl() {
        if (this.manualPowerBiUrl) {
            this.powerBiEmbedUrl = this.manualPowerBiUrl;
            this.safePowerBiUrl = this.sanitizer.bypassSecurityTrustResourceUrl(this.manualPowerBiUrl);
            this.viewMode = 'powerbi';
        }
    }

    applyFilters() {
        if (!this.preview) return;
        
        let data = [...this.preview];
        
        // Apply search term
        if (this.searchTerm) {
            const term = this.searchTerm.toLowerCase();
            data = data.filter(row => 
                Object.values(row).some(val => 
                    String(val).toLowerCase().includes(term)
                )
            );
        }
        
        // Apply column filter
        if (this.filterColumn && this.filterValue) {
            const val = this.filterValue.toLowerCase();
            data = data.filter(row => 
                String(row[this.filterColumn]).toLowerCase().includes(val)
            );
        }
        
        this.filteredData = data;
    }

    clearFilters() {
        this.searchTerm = '';
        this.filterColumn = '';
        this.filterValue = '';
        this.filteredData = this.preview || [];
    }

    copyUrl() { 
        navigator.clipboard.writeText(this.powerBiUrl);
    }
}