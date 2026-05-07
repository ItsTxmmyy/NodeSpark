import { Routes } from '@angular/router';
import { LoginComponent } from './login/login';
import { SignupComponent } from './signup/signup';
import { adminGuard } from './guards/admin.guard';
import { AdminPage } from './pages/admin/admin.page';

export const routes: Routes = [
  { 
    path: '', 
    component: LoginComponent 
  },
  { 
    path: 'signup', 
    component: SignupComponent 
  },
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
      import('./pages/analytics/analytics.page').then((m) => m.AnalyticsPage)
  },
  {
    path: 'admin',
    canActivate: [adminGuard],
    component: AdminPage
  },
  { 
    path: '**', 
    redirectTo: '' 
  }
];
