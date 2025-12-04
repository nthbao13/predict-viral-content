# Databricks notebook source
# MOUNT AZURE STORAGE 

# Configuration
STORAGE_ACCOUNT_NAME = "" # ← Paste storage account name from Azure Portal
STORAGE_ACCOUNT_KEY = ""  # ← Paste key from Azure Portal

# Containers to mount
CONTAINERS = {
    "bronze": "/mnt/bronze",
    "silver": "/mnt/silver",
    "gold": "/mnt/gold"
}

# COMMAND ----------

# Mount function
def mount_container(container_name, mount_point):
    """Mount Azure Blob Storage container"""
    
    # Check if already mounted
    try:
        dbutils.fs.ls(mount_point)
        print(f"✅ {mount_point} already mounted")
        return True
    except:
        pass
    
    # Mount
    try:
        print(f"🔄 Mounting {container_name} to {mount_point}...")
        
        dbutils.fs.mount(
            source = f"wasbs://{container_name}@{STORAGE_ACCOUNT_NAME}.blob.core.windows.net",
            mount_point = mount_point,
            extra_configs = {
                f"fs.azure.account.key.{STORAGE_ACCOUNT_NAME}.blob.core.windows.net": STORAGE_ACCOUNT_KEY
            }
        )
        
        print(f"✅ Successfully mounted {mount_point}")
        return True
        
    except Exception as e:
        print(f"❌ Failed to mount {container_name}: {str(e)}")
        return False

# COMMAND ----------

# Mount all containers
print("🚀 Starting mount process...\n")

for container, mount_point in CONTAINERS.items():
    mount_container(container, mount_point)
    print("")

# COMMAND ----------

# Verify mounts
print("=" * 60)
print("📊 MOUNTED FILESYSTEMS")
print("=" * 60)

for mount in dbutils.fs.mounts():
    if "/mnt/" in mount.mountPoint:
        print(f"\n{mount.mountPoint}")
        print(f"  → {mount.source}")

# COMMAND ----------

# Test access to each mount
print("\n" + "=" * 60)
print("🧪 TESTING MOUNT ACCESS")
print("=" * 60)

for container, mount_point in CONTAINERS.items():
    print(f"\n{mount_point}:")
    try:
        files = dbutils.fs.ls(mount_point)
        print(f"  ✅ Accessible ({len(files)} items)")
        
        # Show first few items
        for item in files[:3]:
            print(f"    - {item.name}")
            
    except Exception as e:
        print(f"  ❌ Not accessible: {str(e)}")

# COMMAND ----------