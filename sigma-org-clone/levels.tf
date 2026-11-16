# Walks the source org's workspace/folder tree and recreates it in the target org.
# Terraform cannot recurse, so each folder depth gets its own data source + resource.
# Levels are keyed by the SOURCE inode id (stable + unique even if names repeat).

# ---------- Workspaces ----------
data "sigma_workspaces" "src" {
  provider = sigma.source
}

locals {
  src_workspaces = {
    for w in data.sigma_workspaces.src.workspaces : w.id => w
    if(length(var.workspace_include) == 0 || contains(var.workspace_include, w.name))
    && !contains(var.workspace_exclude, w.name)
  }
}

resource "sigma_workspace" "dst" {
  provider      = sigma.target
  for_each      = local.src_workspaces
  name          = each.value.name
  no_duplicates = true
}

# ---------- Folder depth 1 ----------
data "sigma_files" "l1" {
  provider             = sigma.source
  for_each             = local.src_workspaces
  parent_id            = each.key
  direct_children_only = true
  type_filters         = ["folder"]
}

locals {
  l1 = merge([
    for pid, d in data.sigma_files.l1 : {
      for f in d.files : f.id => {
        name        = f.name
        description = try(nullif(f.description, ""), null)
        parent      = pid
      }
      if !f.is_archived
    }
  ]...)
}
resource "sigma_folder" "l1" {
  provider    = sigma.target
  for_each    = local.l1
  name        = each.value.name
  description = each.value.description
  parent_id   = sigma_workspace.dst[each.value.parent].id
}

# ---------- Folder depth 2 ----------
data "sigma_files" "l2" {
  provider             = sigma.source
  for_each             = local.l1
  parent_id            = each.key
  direct_children_only = true
  type_filters         = ["folder"]
}

locals {
  l2 = merge([
    for pid, d in data.sigma_files.l2 : {
      for f in d.files : f.id => {
        name        = f.name
        description = try(nullif(f.description, ""), null)
        parent      = pid
      }
      if !f.is_archived
    }
  ]...)
}
resource "sigma_folder" "l2" {
  provider    = sigma.target
  for_each    = local.l2
  name        = each.value.name
  description = each.value.description
  parent_id   = sigma_folder.l1[each.value.parent].id
}

# ---------- Folder depth 3 ----------
data "sigma_files" "l3" {
  provider             = sigma.source
  for_each             = local.l2
  parent_id            = each.key
  direct_children_only = true
  type_filters         = ["folder"]
}

locals {
  l3 = merge([
    for pid, d in data.sigma_files.l3 : {
      for f in d.files : f.id => {
        name        = f.name
        description = try(nullif(f.description, ""), null)
        parent      = pid
      }
      if !f.is_archived
    }
  ]...)
}
resource "sigma_folder" "l3" {
  provider    = sigma.target
  for_each    = local.l3
  name        = each.value.name
  description = each.value.description
  parent_id   = sigma_folder.l2[each.value.parent].id
}

# ---------- Folder depth 4 ----------
data "sigma_files" "l4" {
  provider             = sigma.source
  for_each             = local.l3
  parent_id            = each.key
  direct_children_only = true
  type_filters         = ["folder"]
}

locals {
  l4 = merge([
    for pid, d in data.sigma_files.l4 : {
      for f in d.files : f.id => {
        name        = f.name
        description = try(nullif(f.description, ""), null)
        parent      = pid
      }
      if !f.is_archived
    }
  ]...)
}
resource "sigma_folder" "l4" {
  provider    = sigma.target
  for_each    = local.l4
  name        = each.value.name
  description = each.value.description
  parent_id   = sigma_folder.l3[each.value.parent].id
}

# ---------- Folder depth 5 ----------
data "sigma_files" "l5" {
  provider             = sigma.source
  for_each             = local.l4
  parent_id            = each.key
  direct_children_only = true
  type_filters         = ["folder"]
}

locals {
  l5 = merge([
    for pid, d in data.sigma_files.l5 : {
      for f in d.files : f.id => {
        name        = f.name
        description = try(nullif(f.description, ""), null)
        parent      = pid
      }
      if !f.is_archived
    }
  ]...)
}
resource "sigma_folder" "l5" {
  provider    = sigma.target
  for_each    = local.l5
  name        = each.value.name
  description = each.value.description
  parent_id   = sigma_folder.l4[each.value.parent].id
}

# ---------- Folder depth 6 ----------
data "sigma_files" "l6" {
  provider             = sigma.source
  for_each             = local.l5
  parent_id            = each.key
  direct_children_only = true
  type_filters         = ["folder"]
}

locals {
  l6 = merge([
    for pid, d in data.sigma_files.l6 : {
      for f in d.files : f.id => {
        name        = f.name
        description = try(nullif(f.description, ""), null)
        parent      = pid
      }
      if !f.is_archived
    }
  ]...)
}

# Fail loudly if the source tree is deeper than we copied (add another level to extend).
check "tree_depth" {
  assert {
    condition     = length(local.l6) == 0
    error_message = "Source has folders nested deeper than 5 levels; add another level block to levels.tf."
  }
}

output "created_workspaces" {
  value = { for k, w in sigma_workspace.dst : w.name => w.id }
}

output "folder_counts" {
  value = {
    level_1 = length(local.l1)
    level_2 = length(local.l2)
    level_3 = length(local.l3)
    level_4 = length(local.l4)
    level_5 = length(local.l5)
  }
}
