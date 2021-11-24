variable "cluster_identifier" {
  description = "The Cluster Identifier."
  type = string
}
variable "database_name" {
  description = "The name of the first database to be created when the cluster is created. "
  type = string
}
variable "master_username" {
  description = "Username for the master DB user."
  type = string
}

variable "master_password" {
  description = "Password for the master DB user."
  type = string
}

variable "node_type" {
  description = "The node type to be provisioned for the cluster."
  type = string
}

variable "cluster_type" {
  description = "The cluster type to use."
  type = string
}

variable "number_of_nodes" {
  description = "Number of nodes in the cluster"
  type = number
}

variable "skip_final_snapshot"{
  description = "If true (default), no snapshot will be made before deleting DB"
  type = bool
}