#!/usr/bin/env bash
# Semantically reject mutable workflow dependencies and ambiguous YAML mappings.
set -euo pipefail

cd "$(dirname "$0")/.."

WORKFLOW_ROOT="${1:-.github/workflows}"
test -d "$WORKFLOW_ROOT" || {
  printf 'workflow directory not found: %s\n' "$WORKFLOW_ROOT" >&2
  exit 2
}
command -v ruby >/dev/null 2>&1 || {
  echo "ruby with the standard Psych YAML parser is required" >&2
  exit 127
}

ruby - "$WORKFLOW_ROOT" <<'RUBY'
require "psych"

root = ARGV.fetch(0)
violations = []

def walk(node, &block)
  yield node
  children = node.respond_to?(:children) ? node.children : nil
  children.each { |child| walk(child, &block) } if children
end

def scalar_value(node, anchors, path, violations)
  if node.is_a?(Psych::Nodes::Alias)
    target = anchors[node.anchor]
    unless target
      violations << "#{path}:#{node.start_line + 1}: unresolved YAML alias *#{node.anchor}"
      return nil
    end
    return scalar_value(target, anchors, path, violations)
  end
  unless node.is_a?(Psych::Nodes::Scalar)
    violations << "#{path}:#{node.start_line + 1}: mapping keys and uses values must be scalars"
    return nil
  end
  node.value
end

def valid_reference?(reference)
  if reference.start_with?("./")
    components = reference.delete_prefix("./").split("/")
    return !components.empty? && components.none? { |part| part.empty? || part == "." || part == ".." }
  end
  if reference.start_with?("docker://")
    return !!(reference =~ /\Adocker:\/\/.+@sha256:[0-9a-f]{64}\z/)
  end
  return false unless reference =~ /\A[^@\s]+@[0-9a-f]{40}\z/
  repository, = reference.split("@", 2)
  components = repository.split("/")
  components.length >= 2 && components.none? { |part| part.empty? || part == "." || part == ".." }
end

paths = Dir.glob(File.join(root, "**", "*.{yml,yaml}")).sort
paths.each do |path|
  begin
    stream = Psych.parse_stream(File.read(path, encoding: "UTF-8"), filename: path)
  rescue Psych::SyntaxError, ArgumentError => error
    violations << "#{path}: invalid YAML: #{error.message.lines.first.to_s.strip}"
    next
  end
  unless stream.children.length == 1
    violations << "#{path}: workflow must contain exactly one YAML document"
    next
  end
  document_root = stream.children.first.children.first
  unless document_root.is_a?(Psych::Nodes::Mapping)
    violations << "#{path}: workflow YAML root must be a mapping"
    next
  end

  anchors = {}
  walk(stream) do |node|
    next if node.is_a?(Psych::Nodes::Alias)
    next unless node.respond_to?(:anchor) && node.anchor
    if anchors.key?(node.anchor)
      violations << "#{path}:#{node.start_line + 1}: duplicate YAML anchor &#{node.anchor}"
    else
      anchors[node.anchor] = node
    end
  end

  walk(stream) do |node|
    if node.is_a?(Psych::Nodes::Alias) && !anchors.key?(node.anchor)
      violations << "#{path}:#{node.start_line + 1}: unresolved YAML alias *#{node.anchor}"
    end
    next unless node.is_a?(Psych::Nodes::Mapping)
    seen = {}
    node.children.each_slice(2) do |key_node, value_node|
      key = scalar_value(key_node, anchors, path, violations)
      next if key.nil?
      if seen.key?(key)
        violations << "#{path}:#{key_node.start_line + 1}: duplicate YAML key #{key.inspect}"
      else
        seen[key] = key_node.start_line
      end
      next unless key == "uses"
      reference = scalar_value(value_node, anchors, path, violations)
      next if reference.nil?
      unless valid_reference?(reference)
        violations << "#{path}:#{value_node.start_line + 1}: mutable or invalid workflow dependency #{reference.inspect}"
      end
    end
  end
end

unless violations.empty?
  warn "workflow YAML must be unambiguous and dependencies must use immutable pins:"
  warn violations.uniq.join("\n")
  exit 1
end

puts "PASS  semantic YAML and immutable workflow dependency pins (#{root})"
RUBY
