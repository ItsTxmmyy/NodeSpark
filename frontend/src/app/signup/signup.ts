import { Component } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Router, RouterLink } from '@angular/router';
import { FormsModule } from '@angular/forms';
import { CommonModule } from '@angular/common';

@Component({
  selector: 'app-signup',
  standalone: true,
  imports: [CommonModule, FormsModule, RouterLink],
  templateUrl: './signup.html',
  styleUrl: '../login/login.css' // We can reuse your login styles!
})
export class SignupComponent {
  username = '';
  password = '';
  errorMessage = '';

  constructor(private http: HttpClient, private router: Router) {}

  onSignup() {
    const userData = { username: this.username, password: this.password };
    
    // This calls your FastAPI endpoint
    this.http.post('http://localhost:8000/signup', userData).subscribe({
      next: () => {
        // After signing up, send them to login
        this.router.navigate(['/']);
      },
      error: (err) => {
        this.errorMessage = 'Registration failed. User might already exist.';
      }
    });
  }
}