import { CanActivateFn, Router } from '@angular/router';
import { inject } from '@angular/core';
import { SessionService } from '../session.service';

export const adminGuard: CanActivateFn = async () => {
  const router = inject(Router);
  const session = inject(SessionService);

  const token = localStorage.getItem('token');
  if (!token) {
    await router.navigate(['/']);
    return false;
  }

  const ok = await session.isAdmin();
  if (!ok) {
    await router.navigate(['/data-engineering']);
    return false;
  }

  return true;
};

