import { Routes } from '@angular/router';

export const routes: Routes = [
  {
    path: '',
    pathMatch: 'full',
    redirectTo: 'data-engineering'
  },
  {
    path: 'data-engineering',
    loadComponent: () =>
      import('./pages/data-engineering/data-engineering.page').then(
        (m) => m.DataEngineeringPage
      )
  },
  {
    path: 'analytics',
    loadComponent: () =>
      import('./pages/analytics/analytics.page').then(m => m.AnalyticsPage)
  }
];
