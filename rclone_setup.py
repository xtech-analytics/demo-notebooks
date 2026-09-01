import platform
import shutil


def check_rclone():
    rclone = shutil.which("rclone")

    if rclone:
        print(f"rclone installed: {rclone}")
        return True

    system = platform.system()

    print(f"rclone is not installed.")
    print(f"Operating system: {system}")

    if system == "Linux":
        print("Install with:")
        print("curl https://rclone.org/install.sh | sudo bash")

    elif system == "Windows":
        print("Install rclone using the Windows installer/package manager.")

    elif system == "Darwin":
        print("Install using Homebrew:")
        print("brew install rclone")

    else:
        print("Unsupported operating system.")

    return False


check_rclone()