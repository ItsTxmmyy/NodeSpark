import { Component, signal } from '@angular/core';
import { RouterLink, RouterLinkActive, RouterOutlet } from '@angular/router';

@Component({
  selector: 'app-root',
  imports: [RouterOutlet, RouterLink, RouterLinkActive],
  template: `
    <div class="shell">
      <div class="topbar">
        <div class="brand">NodeSpark</div>
        <nav class="nav">
          <a routerLink="/data-engineering" routerLinkActive="active"
            >Data Engineering</a
          >
        </nav>
      </div>

      <router-outlet />
    </div>
  `,
  styleUrl: './app.css'
})
export class App {
  protected readonly title = signal('frontend');
}
