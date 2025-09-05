import os

EXCLUDED_FOLDERS = {"venv", ".env", ".git"}

def print_tree(directory: str, prefix: str = ""):
    """
    Recursively prints the directory structure like a tree,
    excluding folders defined in EXCLUDED_FOLDERS.
    """
    try:
        entries = sorted(os.listdir(directory))
    except PermissionError:
        # Skip directories without permission
        return

    total_entries = len(entries)
    for index, entry in enumerate(entries):
        path = os.path.join(directory, entry)

        # Skip excluded folders
        if os.path.isdir(path) and entry in EXCLUDED_FOLDERS:
            continue

        connector = "├── " if index < total_entries - 1 else "└── "
        print(prefix + connector + entry)

        # If it's a folder, recursively print its contents
        if os.path.isdir(path):
            new_prefix = prefix + ("│   " if index < total_entries - 1 else "    ")
            print_tree(path, new_prefix)

if __name__ == "__main__":
    folder_path = input("Enter the folder path: ").strip()

    if os.path.isdir(folder_path):
        print(f"\n📂 Directory structure of: {folder_path}\n")
        print_tree(folder_path)
    else:
        print("❌ Invalid path. Please enter a valid directory.")
