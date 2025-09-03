# 🧑‍💻 Contributing to the Course Repository

Welcome! 👋  
If you're a student in the course, this guide will walk you through the steps to submit your assignments via GitHub.

---

## 📦 Step 1: Fork the Repository

1. Click the **"Fork"** button at the top-right corner of this page.
2. Choose your own GitHub account as the destination.
3. Wait for GitHub to create a copy of the repository under your account.

---

## 💻 Step 2: Clone Your Fork

After forking, open your terminal and run:

```bash
git clone https://github.com/<your-username>/<repo-name>.git
cd <repo-name>
```

> 🔁 Replace `<your-username>` and `<repo-name>` with your actual GitHub username and the repository name.

---

## 🌱 Step 3: Create a Branch (Optional but Recommended)

```bash
git checkout -b assignment-1
```

---

## ✏️ Step 4: Work on the Assignment

Follow the instructions in the assignment's `README.md` file.  
Make sure your code is:

- Well-structured
- Inside the correct folder (e.g., `assignments/warm-up`)
- Follows our [Development Guidelines](DEVELOPMENT_GUIDELINES.md)
- Includes comments or documentation if needed

---

## 📤 Step 5: Commit and Push

After completing your work:

```bash
git add .
git commit -m "feat: add lady gaga crawler assignment"
git push origin assignment-1
```

> 💡 **Tip:** Follow our [commit message conventions](DEVELOPMENT_GUIDELINES.md#-commit-message-conventions) for consistent, professional commits.

---

## 📬 Step 6: Open a Pull Request

1. Go to your fork on GitHub.
2. Click **"Compare & pull request"**.
3. Add a title and short description of what you did.
4. Add a label with your name to the pull request.
5. Click **"Create pull request"**.

That's it! 🎉

---

## 🧪 Before Submitting

✅ Did you test your code locally?  
✅ Is it inside the correct folder?  
✅ Did you write a meaningful commit message?

---

## 📅 Deadlines

Make sure to submit your pull request **before the deadline** listed in the assignment instructions.

---

If you have any questions, feel free to open an Issue or ask in class!

Happy coding! 💻✨
