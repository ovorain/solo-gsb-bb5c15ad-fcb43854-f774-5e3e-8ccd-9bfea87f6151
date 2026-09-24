"""Pydantic 请求/响应模型。"""
from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, Field, field_validator

PositiveInt = Annotated[int, Field(ge=1)]


class BatchIn(BaseModel):
    roll_no: str = Field(..., min_length=1, description="胶卷编号")
    expected_start: int = Field(..., ge=0, description="预期帧号起始（含）")
    expected_end: int = Field(..., ge=0, description="预期帧号结束（含）")
    frames: list[int] = Field(..., min_length=1, description="本批扫描帧号列表")
    batch_id: str | None = Field(
        None, description="客户端批次号（可选）；留空则按内容生成"
    )

    @field_validator("roll_no", "batch_id")
    @classmethod
    def _strip(cls, v: str | None) -> str | None:
        if v is None:
            return None
        v = v.strip()
        return v or None

    @field_validator("expected_end")
    @classmethod
    def _check_range(cls, v: int, info) -> int:
        start = info.data.get("expected_start")
        if start is not None and v < start:
            raise ValueError("expected_end 不能小于 expected_start")
        return v


class BatchResult(BaseModel):
    batch_id: str
    roll_no: str
    repeated: bool = Field(..., description="True=内容相同的批次已导入过，本次未做任何更改")
    added_frames: list[int]
    duplicates: list[int] = Field(..., description="重复帧（批内重复 + 胶卷已有），只计一次")
    merged_frames: list[int] = Field(..., description="导入后该胶卷的全部帧号（去重排序）")
    expected_ranges: list[list[int]]
    missing_ranges: list[list[int]] = Field(..., description="预期范围内的连续缺帧区间")
    out_of_range_frames: list[int] = Field(..., description="落在全部预期范围之外的帧")
    created_at: str | None = None


class RollState(BaseModel):
    roll_no: str
    expected_ranges: list[list[int]]
    frames: list[int]
    missing_ranges: list[list[int]]
    out_of_range_frames: list[int]
    total_frames: int
    batch_count: int
    batches: list["BatchMeta"]


class BatchMeta(BaseModel):
    batch_id: str
    expected_start: int
    expected_end: int
    frames: list[int]
    added_frames: list[int]
    duplicates: list[int]
    created_at: str


class RollSummary(BaseModel):
    roll_no: str
    total_frames: int
    batch_count: int
    missing_ranges: list[list[int]]


class FrameProvenanceBatch(BaseModel):
    batch_id: str
    occurrence_count: int | None = Field(
        ...,
        description="该帧在本批原始帧列表中的出现次数；旧批次无原始列表时为 null（不据去重集合推断）",
    )


class FrameProvenance(BaseModel):
    frame_no: int
    first_batch_id: str = Field(
        ...,
        description="当前最早有效批次（首扫归属）；批次撤销后随剩余批次实时转移",
    )
    in_expected_coverage: bool = Field(
        ..., description="该帧是否位于当前（合并后的）预期覆盖闭区间内"
    )
    batches: list[FrameProvenanceBatch] = Field(
        ..., description="扫到该帧的全部现存批次，按原导入顺序排列"
    )


class FrameProvenanceOut(BaseModel):
    roll_no: str
    start: int
    end: int
    limit: int
    next_cursor: int | None = Field(
        ..., description="下一页游标（本页末帧号）；无更多帧时为 null"
    )
    frames: list[FrameProvenance]


class HealthOut(BaseModel):
    status: str
    db: str


class RescanClaimIn(BaseModel):
    claimant: str = Field(..., min_length=1, description="领取人标识")
    max_frames: PositiveInt = Field(..., description="本次最多领取的帧数（正整数）")
    ttl_seconds: PositiveInt = Field(..., description="认领有效秒数（正整数），到期后帧段自动让出")

    @field_validator("claimant")
    @classmethod
    def _strip_claimant(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("claimant 不能为空")
        return v


class RescanClaimOut(BaseModel):
    id: str
    roll_no: str
    claimant: str
    frame_start: int = Field(..., description="认领帧段起始（含）")
    frame_end: int = Field(..., description="认领帧段结束（含）")
    frame_count: int
    max_frames: int
    ttl_seconds: int
    created_at: str
    expires_at: str
    remaining_missing_ranges: list[list[int]] = Field(
        ..., description="该任务帧段中目前仍缺失（未扫到）的连续闭区间"
    )


class RescanReceiptIn(BaseModel):
    batch_id: str = Field(..., min_length=1, description="本次补扫批次号（必填）")
    frames: list[int] = Field(
        ..., min_length=1, description="本次实际扫到的非空帧号列表（原始列表，至少 1 个）"
    )

    @field_validator("batch_id")
    @classmethod
    def _strip_batch_id(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("batch_id 不能为空")
        return v


class RescanReceiptOut(BaseModel):
    id: str = Field(..., description="回执 ID")
    claim_id: str
    roll_no: str
    claimant: str
    frame_start: int = Field(..., description="认领帧段起始（含）")
    frame_end: int = Field(..., description="认领帧段结束（含）")
    batch_id: str = Field(..., description="本次补扫导入的批次号")
    frames: list[int] = Field(
        ..., description="提交的原始帧列表（含批内重复，保序）"
    )
    added_frames: list[int] = Field(..., description="本批新补齐的帧号（去重排序）")
    duplicates: list[int] = Field(
        ..., description="重复帧（批内重复 + 胶卷已有），只计一次"
    )
    repeated: bool = Field(
        ...,
        description="True=相同认领、批次号与原始帧列表的重试，回执为已存记录，未重复导入",
    )
    remaining_missing_ranges: list[list[int]] = Field(
        ..., description="结案后该认领帧段内仍缺失（未扫到）的连续闭区间"
    )
    created_at: str


class RollSnapshotOut(BaseModel):
    snapshot_id: str = Field(..., description="快照 ID，可长期用于差异查询")
    roll_no: str
    expected_ranges: list[list[int]] = Field(
        ..., description="固化时的预期覆盖闭区间（合并后）"
    )
    frames: list[int] = Field(..., description="固化时的实扫帧（去重排序）")
    missing_ranges: list[list[int]] = Field(
        ..., description="固化时的缺帧闭区间（预期范围内未扫到的连续段）"
    )
    created_at: str = Field(..., description="快照创建时间（UTC ISO-8601）")


class RangeDiff(BaseModel):
    added: list[list[int]] = Field(
        ..., description="当前相对快照**新增**的闭区间（当前有、快照无）"
    )
    removed: list[list[int]] = Field(
        ..., description="当前相对快照**消失**的闭区间（快照有、当前无）"
    )


class SnapshotDiffOut(BaseModel):
    snapshot_id: str
    roll_no: str
    roll_exists: bool = Field(
        ...,
        description="查询时胶卷是否存在（最后一批被撤销后为 false，此时当前状态按空处理）",
    )
    snapshot_created_at: str
    compared_at: str = Field(..., description="差异计算时间（UTC ISO-8601）")
    expected: RangeDiff = Field(..., description="预期覆盖的差异（区间集合运算）")
    frames: RangeDiff = Field(..., description="实扫帧的差异（帧集合先压成闭区间再比较）")
    missing: RangeDiff = Field(..., description="缺帧区间的差异（区间集合运算）")


RollState.model_rebuild()
