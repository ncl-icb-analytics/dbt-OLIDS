# Legacy vs New Cluster ID Mapping Analysis

Based on analysis of legacy intermediate models and current test failures, here's the mapping of what cluster IDs were actually used vs what our new models expect:

## Key Findings - Mismatches that need fixing:

### 1. Depression Models
**Legacy used:** `DEPR_COD`, `DEPRES_COD`
**New models expect:** `DEP_COD`, `DEPRES_COD`
**Fix needed:** Change `DEP_COD` → `DEPR_COD` in new depression models

### 2. Learning Disability Models  
**Legacy used:** `LD_DIAGNOSIS_COD`
**New models expect:** `LD_COD`, `LDRES_COD`
**Fix needed:** Change to `LD_DIAGNOSIS_COD` in new learning disability models

### 3. Asthma Medication Models
**Legacy used:** `ASTTRT_COD` (asthma treatment)
**New models expect:** `ASTRX_COD` (corrected cluster ID)
**Fix applied:** Changed from `ASTTRT_COD` to `ASTRX_COD` ✅ COMPLETED

### 4. Dementia Models
**Legacy used:** `DEM_COD` (dementia diagnosis only)
**New models expected:** `DEMRES_COD, DEM_COD`
**Fix applied:** Changed to use only `DEM_COD` (no resolution codes as dementia is permanent) ✅ COMPLETED

### 5. Stroke/TIA Models  
**Legacy used:** `STRK_COD, TIA_COD` (separate stroke and TIA codes)
**New models expected:** `STIARES_COD, STIA_COD`
**Fix applied:** Changed to use `STRK_COD, TIA_COD` to match legacy ✅ COMPLETED

### 6. SMI Models
**Legacy used:** `MH_COD, MHREM_COD` (mental health + remission codes)
**New models expected:** `SMIRES_COD, SMI_COD`
**Fix applied:** Changed to use `MH_COD, MHREM_COD` to match legacy ✅ COMPLETED

### 7. Cancer Models
**Legacy used:** `CAN_COD`, `MDRV_COD`, `CANINVITE_COD`, `CANPCADEC_COD`, `CANPCAPU_COD`, `CANPCSUPP_COD`
**New models expect:** `CAN_COD`, `CANRES_COD`
**Analysis:** Legacy had no `CANRES_COD` - this may not exist in codesets

### 5. Diabetes Models (for comparison)
**Legacy used:** `DM_COD`, `DMRES_COD`
**New models:** Need to verify they use `DM_COD`, `DMRES_COD`

### 6. Mental Health/SMI Models
**Legacy used:** `MH_COD`, `MHREM_COD`
**New models expect:** `SMI_COD`, `SMIRES_COD`
**Analysis:** These appear to be different conditions entirely

### 7. Heart Failure Models
**Legacy used:** `HF_COD`, `HFRES_COD`, `HFLVSD_COD`, `REDEJCFRAC_COD`
**New models:** Need to verify

### 8. NHS Health Check
**Legacy used:** `HEALTH_CHECK_COMP`
**New models expect:** `NHSHEALTHCHECK_COD`
**Fix needed:** Change to `HEALTH_CHECK_COMP`

### 9. Spirometry
**Legacy used:** Various (`FEV1FVCL70_COD`, `FEV1FVC_COD`)
**New models expect:** `UNABLESPI_COD`
**Analysis:** `UNABLESPI_COD` was used in `intermediate_copd_unable_spirometry.sql`

### 10. Stroke/TIA
**Legacy patterns suggest:** Likely `STIA_COD` or similar
**New models expect:** `STIARES_COD`, `STIA_COD`

### 11. Rheumatoid Arthritis
**New models expect:** `RA_COD`
**Status:** Need to verify if this exists

### 12. ACR (Albumin Creatinine Ratio)
**New models expect:** `ACR_COD`
**Status:** Need to verify if this exists

## Correct Legacy Cluster IDs (for reference):

- **BMI:** `BMIVAL_COD`, `BMI30_COD`
- **Blood Pressure:** Various BP codes 
- **Cholesterol:** `CHOL2_COD`
- **Creatinine:** `CRE_COD`
- **Smoking:** `LSMOK_COD`, `EXSMOK_COD`, `NSMOK_COD`
- **QRisk:** `QRISKSCORE_COD`, `QRISK2_10YEAR`
- **Retinal Screening:** `RETSCREN_COD`
- **Waist Circumference:** `WAIST_COD`
- **Fragility Fractures:** `FF_COD`
- **Osteoporosis:** `OSTEO_COD`, `DXA_COD`, `DXA2_COD`
- **Lithium:** `LIT_COD`, `LITSP_COD`
- **Non-diabetic Hyperglycaemia:** `NDH_COD`, `IGT_COD`, `PRD_COD`

## Action Plan:

1. **High Priority Fixes:** Depression (`DEP_COD` → `DEPR_COD`), Learning Disability (`LD_COD` → `LD_DIAGNOSIS_COD`), NHS Health Check (`NHSHEALTHCHECK_COD` → `HEALTH_CHECK_COMP`)

2. **Investigation Needed:** Cancer resolved codes, SMI vs MH distinction, CYP asthma cluster IDs, ACR codes, RA codes

3. **Verification Needed:** Run investigation model to see what cluster IDs actually exist in combined codesets 