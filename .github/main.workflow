workflow "main" {
  on = "push"
  resolves = ["echo"]
}

action "echo" {
  uses = "docker://alpine"
  runs = ["sh", "-c", "echo Hello"]
}
