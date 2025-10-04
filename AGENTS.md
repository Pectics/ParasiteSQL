# Agent Instructions for PermPacks

## Commit Guidelines
- 每次提交尽量只包含一类变更，做到小步提交。
- 所有提交消息使用`XX: 描述`格式，其中`XX`为中文操作类型（例如`新增`、`修复`、`优化`、`移除`、`重构`等），描述部分使用中文；涉及类名、方法名等专有名词时保持原文格式。
- 常见操作类型：
  - `新增`：添加新功能或文件
  - `修复`：修复bug或问题
  - `优化`：性能优化或代码改进
  - `移除`：删除功能或代码
  - `重构`：代码重构
  - `修改`：修改现有功能
  - `更新`：更新依赖或配置

## Release Guidelines
- 版本号遵循语义化版本控制（Semantic Versioning），格式为`vMAJOR.MINOR.PATCH`（例如`v0.2.0`）。
- 变更记录使用`CHANGELOG.md`，遵循Keep a Changelog格式。
- 变更分类：
  - `新增`：新功能
  - `优化`：性能改进和代码优化
  - `修复`：bug修复
  - `文档`：文档更新
  - `移除`：功能移除

## Pull Request Guidelines
- 每个PR的标题以`[动词] `为前缀，动词使用完整中文动词，并概括PR的整体目的。

## Other Guidelines
- 在变更范围内若存在嵌套的`AGENTS.md`，遵循最具体的约定。
- 若无特殊说明，代码风格遵循项目现有约定。
