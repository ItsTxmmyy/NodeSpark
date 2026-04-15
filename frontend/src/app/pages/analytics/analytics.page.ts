import {Component} from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import {HttpClient} from '@angular/common/http';

@Component({
    selector: 'app-analytics',
    standalone:true,
    imports: [CommonModule, FormsModule],
    templateUrl: './analytics.page.html',
    styleUrls: ['./analytics.page.css']
})
export class AnalyticsPage {
    datasets: any [] = [];
    versions: any[] = [];
    preview: any = null;

    selectedDataset: string | null = null;
    selectedVersion:string | null = null;

    powerBiUrl = ''
    backend = 'http://127.0.0.1:8000';

    constructor( private http: HttpClient) {
        this.loadDatasets();
    }
    loadDatasets() {
        this.http.get<any[]>(`${this.backend}/datasets/`).subscribe(d => this.datasets = d);
    }
    
    onDatasetChange(datasetId: string) {
        this.selectedDataset = datasetId;
        this.preview = null;
        this.selectedVersion = null;

        this.http.get<any[]>(`${this.backend}/datsets/${datasetId}/versions`).subscribe( v => this.versions = v);
    }

    onVersionChange(versionId: string) {
        this.selectedVersion = versionId;
        this.powerBiUrl = `${this.backend}/powerbi/${versionId}`;

        this.http.ger<any[]>(this.powerBiUrl).subscribe(data => this.preview = data);
    }

    copyUrl() { 
        navigator.clipboard.writeText(this.powerBiUrl);
    }
}