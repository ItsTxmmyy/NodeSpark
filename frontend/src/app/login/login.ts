import { Component, signal } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Router, RouterLink } from '@angular/router'; // Added RouterLink
import { FormsModule } from '@angular/forms';
import { CommonModule } from '@angular/common';

@Component({
  selector: 'app-login',
  standalone: true,
  // Added RouterLink to imports so the HTML [routerLink] directive works
  imports: [CommonModule, FormsModule, RouterLink], 
  templateUrl: './login.html',
  styleUrl: './login.css'
})
export class LoginComponent {
  username = '';
  password = '';
  errorMessage = '';

  constructor(private http: HttpClient, private router: Router) {}

  onLogin() {
    const loginData = {
      username: this.username,
      password: this.password
    };

    // Sending the login request to your FastAPI backend
    this.http.post<any>('http://localhost:8000/login', loginData).subscribe({
      next: (response) => {
        // Store the token (if your backend provides one) and navigate to the dashboard
        localStorage.setItem('token', response.access_token);
        this.router.navigate(['/data-engineering']);
      },
      error: (err) => {
        console.error('Login error:', err);
        this.errorMessage = 'Invalid username or password';
      }
    });
  }
}