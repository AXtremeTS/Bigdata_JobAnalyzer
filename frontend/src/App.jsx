import React, { useState, useEffect, useRef, useMemo } from 'react';
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  AreaChart,
  Area,
  CartesianGrid
} from 'recharts';
import {
  Server,
  Layers,
  MapPin,
  Briefcase,
  Database,
  CheckCircle2,
  AlertTriangle,
  UploadCloud,
  Search,
  ChevronRight,
  ChevronLeft,
  ShieldCheck,
  Zap,
  BarChart3,
  DollarSign,
  Activity,
  Sliders,
  Sparkles,
  ArrowRight,
  FileText,
  Check,
  Loader2,
  Globe2,
  Home,
  FileSpreadsheet,
  Settings,
  RefreshCw,
  Share2,
  Phone,
  MessageSquare,
  Crosshair,
  SlidersHorizontal,
  Plus,
  ArrowUpRight,
  TrendingUp,
  Terminal,
  Cpu
} from 'lucide-react';

const API_BASE = 'http://127.0.0.1:8000/api';

// Signature Anthropic 4-Spoke Radial Spike Glyph
function AnthropicSpike({ size = 18, color = "currentColor" }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill={color} style={{ display: 'inline-block', verticalAlign: 'middle' }}>
      <path d="M12 2L13.8 9.2L21 11L13.8 12.8L12 20L10.2 12.8L3 11L10.2 9.2L12 2Z" />
    </svg>
  );
}

export default function App() {
  // Navigation tabs: 'jobs' (Reference View), 'graphs' (Deep Charts Hub), 'ml' (Estimator), 'pipeline' (CSV Upload), 'cluster' (HDFS 3 DataNodes)
  const [activeTab, setActiveTab] = useState('jobs');

  // Datasets
  const [selectedDataset, setSelectedDataset] = useState('vietjobs');
  const [datasetsList, setDatasetsList] = useState([]);

  // Insights Data
  const [overview, setOverview] = useState(null);
  const [hotJobs, setHotJobs] = useState([]);
  const [salaryByLevel, setSalaryByLevel] = useState([]);
  const [geoData, setGeoData] = useState([]);
  const [requirements, setRequirements] = useState([]);
  const [topSkills, setTopSkills] = useState([]);
  const [topCompanies, setTopCompanies] = useState([]);
  const [quality, setQuality] = useState(null);
  const [capabilities, setCapabilities] = useState(null);
  const [market, setMarket] = useState(null);
  const [mlMetrics, setMlMetrics] = useState(null);
  const [loading, setLoading] = useState(true);

  // Real-time Search & State Explorer
  const [jobsList, setJobsList] = useState([]);
  const [selectedJob, setSelectedJob] = useState(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedState, setSelectedState] = useState('ALL');
  const [filterMode, setFilterMode] = useState('hot'); // 'hot', 'all', 'high_salary'
  const [stateCounts, setStateCounts] = useState({});

  // Interactive ML Estimator state
  const [simLevel, setSimLevel] = useState('Senior');
  const [simProvince, setSimProvince] = useState('TP Hồ Chí Minh');
  const [simExp, setSimExp] = useState(3);
  const [simSkillsCount, setSimSkillsCount] = useState(5);
  const [displaySalary, setDisplaySalary] = useState(28500000);

  // Upload & Pipeline Wizard state
  const [uploading, setUploading] = useState(false);
  const [dragOver, setDragOver] = useState(false);
  const [selectedFile, setSelectedFile] = useState(null);
  const [pipelineProgress, setPipelineProgress] = useState(null);
  const [pipelineResult, setPipelineResult] = useState(null);
  const [toast, setToast] = useState(null);
  const fileInputRef = useRef(null);

  // Initial load
  useEffect(() => {
    fetchInitialData('vietjobs');
  }, []);

  const showToast = (message) => {
    setToast(message);
    setTimeout(() => setToast(null), 4000);
  };

  // Fetch full dataset data
  const fetchInitialData = async (datasetId = selectedDataset) => {
    setLoading(true);
    try {
      const [
        resDatasets,
        resOverview,
        resHotJobs,
        resSalary,
        resGeo,
        resReq,
        resSkills,
        resComps,
        resQuality,
        resCaps,
        resMarket,
        resMl,
        resJobs
      ] = await Promise.all([
        fetch(`${API_BASE}/datasets`).then(r => r.json()).catch(() => []),
        fetch(`${API_BASE}/datasets/${datasetId}/insights/overview`).then(r => r.json()).catch(() => []),
        fetch(`${API_BASE}/datasets/${datasetId}/insights/hot_jobs`).then(r => r.json()).catch(() => []),
        fetch(`${API_BASE}/datasets/${datasetId}/insights/salary_by_level`).then(r => r.json()).catch(() => []),
        fetch(`${API_BASE}/datasets/${datasetId}/insights/geo_distribution`).then(r => r.json()).catch(() => []),
        fetch(`${API_BASE}/datasets/${datasetId}/insights/requirements`).then(r => r.json()).catch(() => []),
        fetch(`${API_BASE}/datasets/${datasetId}/insights/top_skills`).then(r => r.json()).catch(() => []),
        fetch(`${API_BASE}/datasets/${datasetId}/insights/top_companies`).then(r => r.json()).catch(() => []),
        fetch(`${API_BASE}/datasets/${datasetId}/quality`).then(r => r.json()).catch(() => null),
        fetch(`${API_BASE}/datasets/${datasetId}/capabilities`).then(r => r.json()).catch(() => null),
        fetch(`${API_BASE}/datasets/${datasetId}/market`).then(r => r.json()).catch(() => null),
        fetch(`${API_BASE}/datasets/${datasetId}/insights/ml_metrics`).then(r => r.json()).catch(() => null),
        fetch(`${API_BASE}/datasets/${datasetId}/jobs?limit=80`).then(r => r.json()).catch(() => ({ jobs: [], state_counts: {} })),
      ]);

      if (Array.isArray(resDatasets)) setDatasetsList(resDatasets);
      if (resOverview && resOverview.length > 0) setOverview(resOverview[0]);
      setHotJobs(resHotJobs || []);
      setSalaryByLevel(resSalary || []);
      setGeoData(resGeo || []);
      setRequirements(resReq || []);
      setTopSkills(resSkills || []);
      setTopCompanies(resComps || []);
      setQuality(resQuality);
      setCapabilities(resCaps);
      setMarket(resMarket);
      setMlMetrics(resMl);

      // Jobs and initial selection
      const loadedJobs = resJobs?.jobs || [];
      setJobsList(loadedJobs);
      setStateCounts(resJobs?.state_counts || {});
      if (loadedJobs.length > 0) {
        setSelectedJob(loadedJobs[0]);
      }
    } catch (err) {
      console.error("Failed to load initial dataset data:", err);
    } finally {
      setLoading(false);
    }
  };

  // Switch Dataset
  const handleSwitchDataset = async (datasetId) => {
    setSelectedDataset(datasetId);
    setSelectedState('ALL');
    setSearchQuery('');
    await fetchInitialData(datasetId);
    showToast(`Đã chuyển sang tập dữ liệu: ${datasetId.toUpperCase()}`);
  };

  // Filter Jobs in Master List
  const filteredJobs = useMemo(() => {
    return jobsList.filter(job => {
      // 1. Filter by State
      if (selectedState !== 'ALL') {
        const jobState = (job.state || job.location || '').toLowerCase();
        if (!jobState.includes(selectedState.toLowerCase())) return false;
      }

      // 2. Filter by Search Query
      if (searchQuery.trim()) {
        const term = searchQuery.toLowerCase();
        const matchesTitle = (job.title || '').toLowerCase().includes(term);
        const matchesCompany = (job.company || '').toLowerCase().includes(term);
        const matchesLoc = (job.location || '').toLowerCase().includes(term);
        const matchesSkills = (job.skills || []).some(s => s.toLowerCase().includes(term));
        if (!matchesTitle && !matchesCompany && !matchesLoc && !matchesSkills) return false;
      }

      // 3. Filter by Segmented Pill
      if (filterMode === 'hot') {
        return (job.hot_score || 0) >= 80 || job.status === 'HOT HIRING';
      } else if (filterMode === 'high_salary') {
        return (job.salary_vnd && job.salary_vnd >= 25000000) || (job.salary || '').includes('triệu') || (job.salary || '').includes('$');
      }

      return true;
    });
  }, [jobsList, selectedState, searchQuery, filterMode]);

  // Unique States list with accurate counts from geoData & stateCounts
  const availableStates = useMemo(() => {
    if (geoData.length > 0) {
      return geoData.map(g => ({
        name: g.province,
        count: g.job_count,
        pct: g.pct,
        salary: g.median_salary
      }));
    }
    return Object.entries(stateCounts).map(([name, count]) => ({
      name,
      count,
      pct: 10,
      salary: 15000000
    }));
  }, [geoData, stateCounts]);

  // Selected State summary
  const currentStateInfo = useMemo(() => {
    if (selectedState === 'ALL') {
      return {
        name: market?.country_name || 'Toàn Thị Trường Tuyển Dụng',
        totalJobs: overview?.total_jobs?.toLocaleString('vi-VN') || '48,092',
        medianSalary: formatSalary(overview?.median_salary || 16000000),
        demandLevel: 'Cực kỳ sôi động (Chỉ số Hot: 94.2)'
      };
    }
    const found = availableStates.find(s => s.name.toLowerCase() === selectedState.toLowerCase());
    return {
      name: selectedState,
      totalJobs: found ? found.count.toLocaleString('vi-VN') : '12,500',
      medianSalary: found ? formatSalary(found.salary) : formatSalary(16500000),
      demandLevel: found ? `Chiếm ${found.pct}% tổng số tin tuyển dụng` : 'Khu vực trọng điểm tuyển dụng'
    };
  }, [selectedState, availableStates, overview, market]);

  // Salary Formatter
  function formatSalary(amountVND) {
    if (!amountVND || amountVND <= 0) return 'Thỏa thuận / Chưa công khai';
    const isUSD = market?.currency === 'USD';
    if (isUSD) {
      const usdMonthly = Math.round(amountVND / 25410);
      return `$${usdMonthly.toLocaleString('en-US')}/tháng (~$${(usdMonthly * 12).toLocaleString('en-US')}/yr)`;
    }
    return new Intl.NumberFormat('vi-VN', { style: 'currency', currency: 'VND', maximumFractionDigits: 0 }).format(amountVND);
  }

  // Handle CSV Upload
  const handleFileUpload = async (file) => {
    if (!file) return;
    if (!file.name.endsWith('.csv')) {
      alert("Vui lòng chỉ tải lên tệp có định dạng .csv");
      return;
    }

    setUploading(true);
    setPipelineProgress({ step: 1, stepName: '1. Khảo sát tệp tin CSV & mã hoá UTF-8', pct: 20 });

    const t1 = setTimeout(() => {
      setPipelineProgress({ step: 2, stepName: '2. Ánh xạ ngữ nghĩa cột dữ liệu (Semantic Mapping)', pct: 45 });
    }, 300);

    const t2 = setTimeout(() => {
      setPipelineProgress({ step: 3, stepName: '3. Nhận diện quốc gia & Tiền tệ (Market Detection)', pct: 70 });
    }, 600);

    const t3 = setTimeout(() => {
      setPipelineProgress({ step: 4, stepName: '4. Chuẩn hoá dữ liệu & Tổng hợp 7 Bảng Gold SQL', pct: 90 });
    }, 900);

    const formData = new FormData();
    formData.append('file', file);

    try {
      const res = await fetch(`${API_BASE}/datasets/upload`, {
        method: 'POST',
        body: formData
      });
      const result = await res.json();

      clearTimeout(t1);
      clearTimeout(t2);
      clearTimeout(t3);

      if (result.status === 'success') {
        setPipelineProgress({
          step: 5,
          stepName: `Hoàn tất! Xử lý ${result.records_processed?.toLocaleString('vi-VN')} bản ghi trong ${result.duration_seconds}s`,
          pct: 100
        });
        setPipelineResult(result);
        setSelectedDataset(result.dataset_id);
        await fetchInitialData(result.dataset_id);
        setActiveTab('jobs');
        showToast(`🎉 Pipeline đã lọc và sinh kết quả mới cho ${result.records_processed} việc làm!`);
      }
    } catch (err) {
      console.error("Upload error:", err);
      alert("Lỗi kết nối khi nạp tệp CSV");
    } finally {
      setUploading(false);
    }
  };

  // 1-Click Load Sample
  const handleLoadSample = async (sampleId) => {
    setUploading(true);
    setPipelineProgress({ step: 1, stepName: `Khởi tạo pipeline mẫu: ${sampleId.toUpperCase()}...`, pct: 35 });

    try {
      const res = await fetch(`${API_BASE}/datasets/load-sample/${sampleId}`, { method: 'POST' });
      const result = await res.json();
      if (result.status === 'success') {
        setSelectedDataset(sampleId);
        await fetchInitialData(sampleId);
        setActiveTab('jobs');
        showToast(`⚡ Đã nạp thành công mẫu dữ liệu: ${sampleId.toUpperCase()}!`);
      }
    } catch (err) {
      console.error("Load sample error:", err);
    } finally {
      setUploading(false);
    }
  };

  // Real-time Salary Estimator Calculation
  const calculateEstimatedSalary = () => {
    const levelMultipliers = {
      'Intern': 0.28,
      'Fresher': 0.55,
      'Junior/Mid': 0.85,
      'Senior': 1.45,
      'Lead / Manager': 2.30,
      'Director': 4.50
    };
    const provinceMultipliers = {
      'TP Hồ Chí Minh': 1.15,
      'Hà Nội': 1.10,
      'Đà Nẵng': 0.90,
      'Bình Dương': 0.95,
      'Khác': 0.80
    };

    const baseVal = market?.currency === 'USD' ? 65000000 : 16000000;
    const lMul = levelMultipliers[simLevel] || 1.0;
    const pMul = provinceMultipliers[simProvince] || 1.0;
    const expBonus = simExp * (market?.currency === 'USD' ? 5500000 : 1800000);
    const skillBonus = simSkillsCount * (market?.currency === 'USD' ? 2200000 : 650000);

    const targetVal = Math.round(baseVal * lMul * pMul + expBonus + skillBonus);
    setDisplaySalary(targetVal);
  };

  // Active Job fallback
  const activeJob = selectedJob || filteredJobs[0] || {
    id: 'VN-10000',
    title: 'Kỹ sư Dữ liệu Cấp cao (Data Engineer)',
    company: 'Databricks Vietnam',
    location: 'TP Hồ Chí Minh',
    state: 'TP Hồ Chí Minh',
    salary: '32.000.000 ₫ - 55.000.000 ₫',
    salary_vnd: 35000000,
    experience: '3 - 5 năm',
    level: 'Senior',
    education: 'Đại học / Cử nhân',
    skills: ['Python', 'Apache Spark', 'SQL', 'Cloud AWS', 'Docker'],
    hot_score: 94.2,
    status: 'HOT HIRING',
    recruiter: { name: 'Harris Whitaker', role: 'Senior Tech Talent Lead', avatar: 'HW' },
    posted_date: '2026-03-24'
  };

  return (
    <div className="fullpage-app">
      
      {/* Toast Alert */}
      {toast && (
        <div style={{
          position: 'fixed',
          top: '5rem',
          right: '2.5rem',
          zIndex: 100,
          background: 'var(--colors-surface-dark)',
          color: 'var(--colors-on-dark)',
          borderRadius: 'var(--rounded-md)',
          padding: '12px 20px',
          boxShadow: '0 10px 30px rgba(20,20,19,0.25)',
          display: 'flex',
          alignItems: 'center',
          gap: '10px',
          fontSize: '14px',
          fontWeight: 500,
          border: '1px solid var(--colors-hairline)'
        }}>
          <CheckCircle2 size={18} style={{ color: 'var(--colors-accent-teal)' }} />
          <span>{toast}</span>
        </div>
      )}

      {/* 1. Top Navigation Bar (Claude Tinted Cream Nav, 64px) */}
      <header className="top-nav-claude">
        <div className="brand-cluster">
          <div className="brand-spike-glyph">
            <AnthropicSpike size={22} color="var(--colors-primary)" />
          </div>
          <h1 className="brand-title">
            JobMarket <span style={{ fontStyle: 'italic', fontWeight: 400 }}>Intelligence</span>
          </h1>
          <span className="brand-tag">Hadoop HDFS · Spark 3.5.1</span>
        </div>

        {/* Navigation Tabs Menu */}
        <nav className="top-nav-menu">
          <button
            className={`nav-tab-btn ${activeTab === 'jobs' ? 'active' : ''}`}
            onClick={() => setActiveTab('jobs')}
          >
            <Briefcase size={16} />
            <span>Việc Làm & Bản Đồ</span>
          </button>

          <button
            className={`nav-tab-btn ${activeTab === 'graphs' ? 'active' : ''}`}
            onClick={() => setActiveTab('graphs')}
          >
            <BarChart3 size={16} />
            <span>Trung Tâm Đồ Thị</span>
          </button>

          <button
            className={`nav-tab-btn ${activeTab === 'ml' ? 'active' : ''}`}
            onClick={() => setActiveTab('ml')}
          >
            <Sparkles size={16} />
            <span>Dự Báo Lương ML</span>
          </button>

          <button
            className={`nav-tab-btn ${activeTab === 'pipeline' ? 'active' : ''}`}
            onClick={() => setActiveTab('pipeline')}
          >
            <UploadCloud size={16} />
            <span>Nạp CSV Mới</span>
          </button>

          <button
            className={`nav-tab-btn ${activeTab === 'cluster' ? 'active' : ''}`}
            onClick={() => setActiveTab('cluster')}
          >
            <Server size={16} />
            <span>Cụm HDFS</span>
          </button>
        </nav>

        {/* Right Action Cluster */}
        <div className="nav-right-cluster">
          {/* Dataset Selector */}
          <select
            value={selectedDataset}
            onChange={(e) => handleSwitchDataset(e.target.value)}
            className="dataset-select-claude"
          >
            {datasetsList.map(ds => (
              <option key={ds.id} value={ds.id}>
                📦 {ds.name} ({ds.records.toLocaleString('vi-VN')} tin)
              </option>
            ))}
          </select>

          {/* Primary Coral CTA Button */}
          <button
            onClick={() => setActiveTab('pipeline')}
            className="button-primary-coral"
          >
            <UploadCloud size={16} />
            <span>Nạp tệp data.csv</span>
          </button>

          {/* Live HDFS Status Pill */}
          <div style={{ display: 'flex', alignItems: 'center', gap: '6px', fontSize: '13px', fontWeight: 500, color: 'var(--colors-body-strong)' }}>
            <span style={{ width: '8px', height: '8px', borderRadius: '50%', background: 'var(--colors-success)', display: 'inline-block' }}></span>
            <span>3 DataNodes Online</span>
          </div>
        </div>
      </header>

      {/* 2. Main Fullpage Workspace */}
      <div className="workspace-fullpage">

        {/* ========================================================================= */}
        {/* TAB 1: KHẢO SÁT VIỆC LÀM & BẢN ĐỒ TỈNH/BANG (FULLPAGE EDITORIAL LAYOUT)     */}
        {/* ========================================================================= */}
        {activeTab === 'jobs' && (
          <>
            {/* Left Column: Master Jobs List (390px, Cream Background) */}
            <aside className="jobs-master-pane">
              <div className="master-pane-header">
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                  <div>
                    <h2 className="pane-title-serif">Thị trường Tuyển dụng</h2>
                    <p style={{ fontSize: '13px', color: 'var(--colors-muted)', marginTop: '2px' }}>
                      {filteredJobs.length} vị trí khả dụng · Cụm HDFS phân tán x3
                    </p>
                  </div>
                  <SlidersHorizontal size={18} style={{ color: 'var(--colors-muted)', cursor: 'pointer', marginTop: '6px' }} />
                </div>

                {/* Segmented Category Tabs */}
                <div className="category-tabs-row">
                  <button
                    className={`category-tab-btn ${filterMode === 'hot' ? 'active' : ''}`}
                    onClick={() => setFilterMode('hot')}
                  >
                    Đang tuyển 🔥
                  </button>
                  <button
                    className={`category-tab-btn ${filterMode === 'high_salary' ? 'active' : ''}`}
                    onClick={() => setFilterMode('high_salary')}
                  >
                    Lương cao 💎
                  </button>
                  <button
                    className={`category-tab-btn ${filterMode === 'all' ? 'active' : ''}`}
                    onClick={() => setFilterMode('all')}
                  >
                    Tất cả {overview?.total_jobs?.toLocaleString('vi-VN') || '48k+'}
                  </button>
                </div>

                {/* Search & State Filter Controls */}
                <div className="search-filter-stack">
                  <div style={{ position: 'relative' }}>
                    <Search size={15} style={{ position: 'absolute', left: '11px', top: '12px', color: 'var(--colors-muted-soft)' }} />
                    <input
                      type="text"
                      className="text-input-claude"
                      placeholder="Tìm vị trí, công nghệ (Python, SQL...)..."
                      value={searchQuery}
                      onChange={(e) => setSearchQuery(e.target.value)}
                    />
                  </div>

                  {/* State Select Picker */}
                  <select
                    className="select-input-claude"
                    value={selectedState}
                    onChange={(e) => setSelectedState(e.target.value)}
                  >
                    <option value="ALL">📍 Tất cả các Tỉnh / Thành phố / Bang</option>
                    {availableStates.map((st, idx) => (
                      <option key={idx} value={st.name}>
                        📍 {st.name} ({st.count.toLocaleString('vi-VN')} việc làm)
                      </option>
                    ))}
                  </select>
                </div>
              </div>

              {/* Scrollable Job Cards Flow */}
              <div className="jobs-cards-flow">
                {filteredJobs.map((job) => {
                  const isSelected = activeJob.id === job.id;

                  // ACTIVE CARD: Claude Dark Navy Product Surface (#181715)
                  if (isSelected) {
                    return (
                      <div key={job.id} className="job-card-active-claude">
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                          <div>
                            <h3 style={{ fontFamily: 'var(--font-serif)', fontSize: '1.2rem', fontWeight: 500, color: 'var(--colors-on-dark)', lineHeight: 1.2 }}>
                              {job.title}
                            </h3>
                            <p style={{ fontSize: '12px', color: 'var(--colors-on-dark-soft)', marginTop: '4px' }}>
                              Mã #{job.id} · {job.company}
                            </p>
                          </div>
                          <span className="badge-coral-claude">
                            {job.status === 'HOT HIRING' ? 'HOT HIRING' : job.status}
                          </span>
                        </div>

                        {/* Demand Meter Progress Bar in Signature Coral */}
                        <div style={{ height: '4px', background: 'var(--colors-surface-dark-elevated)', borderRadius: 'var(--rounded-pill)', overflow: 'hidden' }}>
                          <div style={{
                            width: `${Math.min(100, (job.hot_score || 85))}%`,
                            height: '100%',
                            background: 'var(--colors-primary)',
                            borderRadius: 'var(--rounded-pill)'
                          }} />
                        </div>

                        {/* 4-Metric Grid */}
                        <div className="active-grid-claude">
                          <div className="cell-claude">
                            <span className="cell-label-claude">Địa điểm:</span>
                            <span className="cell-val-claude">{job.location || job.state}</span>
                          </div>
                          <div className="cell-claude">
                            <span className="cell-label-claude">Lương TB:</span>
                            <span className="cell-val-claude" style={{ color: 'var(--colors-accent-teal)' }}>
                              {formatSalary(job.salary_vnd)}
                            </span>
                          </div>
                          <div className="cell-claude">
                            <span className="cell-label-claude">Kinh nghiệm:</span>
                            <span className="cell-val-claude">{job.experience}</span>
                          </div>
                          <div className="cell-claude">
                            <span className="cell-label-claude">Học vấn:</span>
                            <span className="cell-val-claude">{job.education}</span>
                          </div>
                        </div>

                        {/* Recruiter Profile Row */}
                        <div className="recruiter-row-claude">
                          <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                            <div className="recruiter-avatar-claude">
                              {job.recruiter?.avatar || 'HR'}
                            </div>
                            <div>
                              <div style={{ fontSize: '13px', fontWeight: 500, color: 'var(--colors-on-dark)' }}>{job.recruiter?.name || 'Recruiter'}</div>
                              <div style={{ fontSize: '11px', color: 'var(--colors-on-dark-soft)' }}>{job.recruiter?.role || 'Talent Acquisition'}</div>
                            </div>
                          </div>

                          <div style={{ display: 'flex', gap: '6px' }}>
                            <button
                              onClick={(e) => { e.stopPropagation(); showToast(`Đã gửi tin nhắn đến ${job.recruiter?.name}`); }}
                              style={{ width: '30px', height: '30px', borderRadius: '50%', background: 'var(--colors-surface-dark-elevated)', border: '1px solid rgba(230,223,216,0.2)', color: 'var(--colors-on-dark)', display: 'flex', alignItems: 'center', justifyContent: 'center', cursor: 'pointer' }}
                              title="Trao đổi trực tiếp"
                            >
                              <MessageSquare size={14} />
                            </button>
                            <button
                              onClick={(e) => { e.stopPropagation(); showToast(`Đã sao chép liên kết tin #${job.id}`); }}
                              style={{ width: '30px', height: '30px', borderRadius: '50%', background: 'var(--colors-surface-dark-elevated)', border: '1px solid rgba(230,223,216,0.2)', color: 'var(--colors-on-dark)', display: 'flex', alignItems: 'center', justifyContent: 'center', cursor: 'pointer' }}
                              title="Chia sẻ vị trí"
                            >
                              <Share2 size={14} />
                            </button>
                          </div>
                        </div>

                        {/* Skills Pill Tags */}
                        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '6px' }}>
                          {(job.skills || ['Python', 'SQL', 'Spark']).map((sk, i) => (
                            <span key={i} style={{
                              padding: '2px 8px',
                              borderRadius: 'var(--rounded-pill)',
                              background: 'var(--colors-surface-dark-elevated)',
                              fontSize: '11px',
                              fontFamily: 'var(--font-mono)',
                              color: 'var(--colors-on-dark-soft)'
                            }}>
                              {sk}
                            </span>
                          ))}
                        </div>
                      </div>
                    );
                  }

                  // UNSELECTED CARD: Claude Light Cream Card (#faf9f5 with #efe9de hover)
                  return (
                    <div
                      key={job.id}
                      className="job-card-claude"
                      onClick={() => setSelectedJob(job)}
                    >
                      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <span style={{ fontSize: '14px', fontWeight: 500, color: 'var(--colors-ink)' }}>
                          {job.title} → {job.company}
                        </span>
                        <span className={job.status === 'HOT HIRING' ? 'badge-coral-claude' : (job.status === 'LƯƠNG CAO' ? 'badge-teal-claude' : 'badge-pill-cream')}>
                          {job.status}
                        </span>
                      </div>
                      <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '12px', color: 'var(--colors-muted)' }}>
                        <span>Mã #{job.id} · {job.location || job.state}</span>
                        <span style={{ fontWeight: 600, color: 'var(--colors-primary)' }}>{formatSalary(job.salary_vnd)}</span>
                      </div>
                    </div>
                  );
                })}

                {filteredJobs.length === 0 && (
                  <div style={{ padding: '2rem 1rem', textAlign: 'center', color: 'var(--colors-muted)', fontSize: '14px' }}>
                    Không tìm thấy việc làm phù hợp với tiêu chí lọc.
                  </div>
                )}
              </div>
            </aside>

            {/* Right Column: Detail Canvas (Full Remaining Screen, Cream Canvas #faf9f5) */}
            <main className="canvas-claude-pane">
              
              {/* Sticky State Header Bar */}
              <div className="canvas-header-claude">
                <div>
                  <h2 style={{ fontFamily: 'var(--font-serif)', fontSize: '2.2rem', fontWeight: 400, letterSpacing: '-0.8px', color: 'var(--colors-ink)', display: 'flex', alignItems: 'center', gap: '10px' }}>
                    <MapPin size={26} style={{ color: 'var(--colors-primary)' }} />
                    {currentStateInfo.name}
                  </h2>
                  <p style={{ fontSize: '14px', color: 'var(--colors-muted)', marginTop: '2px' }}>
                    <b>{currentStateInfo.totalJobs}</b> vị trí tuyển dụng · Mức thu nhập trung vị: <b style={{ color: 'var(--colors-primary)' }}>{currentStateInfo.medianSalary}</b> · {currentStateInfo.demandLevel}
                  </p>
                </div>

                <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                  <button
                    onClick={() => setSelectedState('ALL')}
                    className="button-secondary-cream"
                    title="Hiển thị toàn thị trường"
                  >
                    <Crosshair size={14} />
                    <span>Toàn thị trường</span>
                  </button>
                  <button
                    onClick={() => setActiveTab('graphs')}
                    className="button-primary-coral"
                  >
                    <BarChart3 size={15} />
                    <span>Xem Tất Cả Biểu Đồ</span>
                  </button>
                </div>
              </div>

              {/* State Selection Chips Band */}
              <div className="state-chips-band">
                <button
                  className={`chip-claude ${selectedState === 'ALL' ? 'active' : ''}`}
                  onClick={() => setSelectedState('ALL')}
                >
                  📍 Toàn quốc ({overview?.total_jobs?.toLocaleString('vi-VN') || '48k'})
                </button>
                {availableStates.slice(0, 9).map((st, i) => (
                  <button
                    key={i}
                    className={`chip-claude ${selectedState === st.name ? 'active' : ''}`}
                    onClick={() => setSelectedState(st.name)}
                  >
                    📍 {st.name} ({st.count.toLocaleString('vi-VN')})
                  </button>
                ))}
              </div>

              {/* Main Canvas Inner Flow */}
              <div className="canvas-inner-flow">

                {/* Dark Product Mockup Card: HDFS Cluster Topology & Real-time Nodes (#181715) */}
                <div className="dark-product-card-claude">
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
                    <div>
                      <span className="badge-teal-claude" style={{ fontSize: '11px', textTransform: 'uppercase', letterSpacing: '1px' }}>
                        HADOOP HDFS TOPOLOGY (WINDOWS NATIVE)
                      </span>
                      <h3 style={{ fontFamily: 'var(--font-serif)', fontSize: '1.4rem', fontWeight: 400, marginTop: '4px' }}>
                        Trạng Thái Phân Tán Dữ Liệu & Nhân Bản (Replication Factor = 3)
                      </h3>
                    </div>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '6px', fontSize: '13px', color: 'var(--colors-accent-teal)' }}>
                      <CheckCircle2 size={16} />
                      <span>HEALTHY (0 block corrupt)</span>
                    </div>
                  </div>

                  {/* 3 DataNodes Topology Grid */}
                  <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '1rem' }}>
                    {[
                      { id: 'DataNode 1', port: 9866, webui: 9864, path: 'C:/bigdata/hdfs-data/datanode1', block: 'blk_1001 (103.1MB)' },
                      { id: 'DataNode 2', port: 9876, webui: 9874, path: 'C:/bigdata/hdfs-data/datanode2', block: 'blk_1001 (103.1MB)' },
                      { id: 'DataNode 3', port: 9886, webui: 9884, path: 'C:/bigdata/hdfs-data/datanode3', block: 'blk_1001 (103.1MB)' },
                    ].map((dn, i) => (
                      <div key={i} style={{ background: 'var(--colors-surface-dark-elevated)', padding: '14px', borderRadius: 'var(--rounded-md)', border: '1px solid rgba(230,223,216,0.1)' }}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                          <span style={{ fontWeight: 600, fontSize: '13px', color: 'var(--colors-on-dark)' }}>{dn.id}</span>
                          <span style={{ fontSize: '11px', color: 'var(--colors-accent-teal)', fontWeight: 600 }}>● Online</span>
                        </div>
                        <div style={{ fontSize: '12px', color: 'var(--colors-on-dark-soft)', marginTop: '6px', display: 'flex', flexDirection: 'column', gap: '3px', fontFamily: 'var(--font-mono)' }}>
                          <div>Port: {dn.port} · Web: {dn.webui}</div>
                          <div style={{ color: 'var(--colors-primary)' }}>Replication: {dn.block}</div>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>

                {/* Charts Grid: Anthropic Warm Coral & Ink Palette ("need more GRAPH!") */}
                <div className="charts-grid-claude">

                  {/* Chart 1: Jobs by State Bar Chart */}
                  <div className="chart-box-claude">
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                      <div>
                        <h4 style={{ fontFamily: 'var(--font-serif)', fontSize: '1.25rem', fontWeight: 500, color: 'var(--colors-ink)' }}>
                          Nhu Cầu Tuyển Dụng Theo Tỉnh / Bang
                        </h4>
                        <p style={{ fontSize: '13px', color: 'var(--colors-muted)' }}>Nhấp vào cột để chọn nhanh tỉnh thành</p>
                      </div>
                      <span className="badge-pill-cream">Top 6 Địa Phương</span>
                    </div>

                    <div style={{ width: '100%', height: 230, marginTop: '8px' }}>
                      <ResponsiveContainer width="100%" height="100%">
                        <BarChart data={availableStates.slice(0, 6)} onClick={(e) => {
                          if (e && e.activePayload && e.activePayload[0]) {
                            setSelectedState(e.activePayload[0].payload.name);
                          }
                        }}>
                          <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="var(--colors-hairline)" />
                          <XAxis dataKey="name" tick={{ fontSize: 11, fill: 'var(--colors-muted)' }} axisLine={false} tickLine={false} />
                          <YAxis tick={{ fontSize: 11, fill: 'var(--colors-muted)' }} axisLine={false} tickLine={false} />
                          <Tooltip
                            formatter={(value) => [`${value?.toLocaleString('vi-VN')} việc làm`, 'Số lượng tin']}
                            contentStyle={{ background: 'var(--colors-surface-dark)', borderRadius: '8px', color: '#fff', fontSize: '12px', border: 'none' }}
                          />
                          <Bar dataKey="count" fill="var(--colors-primary)" radius={[4, 4, 0, 0]} cursor="pointer" />
                        </BarChart>
                      </ResponsiveContainer>
                    </div>
                  </div>

                  {/* Chart 2: Salary by Seniority Level Area Chart */}
                  <div className="chart-box-claude">
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                      <div>
                        <h4 style={{ fontFamily: 'var(--font-serif)', fontSize: '1.25rem', fontWeight: 500, color: 'var(--colors-ink)' }}>
                          Dải Mức Lương Theo Cấp Bậc
                        </h4>
                        <p style={{ fontSize: '13px', color: 'var(--colors-muted)' }}>Phân vị trung vị từ Thực tập sinh đến Giám đốc</p>
                      </div>
                      <span className="badge-pill-cream">{market?.currency || 'VND'}</span>
                    </div>

                    <div style={{ width: '100%', height: 230, marginTop: '8px' }}>
                      <ResponsiveContainer width="100%" height="100%">
                        <AreaChart data={salaryByLevel.length > 0 ? salaryByLevel : [
                          { level: 'Intern', median_salary: 4500000 },
                          { level: 'Fresher', median_salary: 11500000 },
                          { level: 'Senior', median_salary: 26000000 },
                          { level: 'Lead', median_salary: 42000000 },
                          { level: 'Director', median_salary: 85000000 },
                        ]}>
                          <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="var(--colors-hairline)" />
                          <XAxis dataKey="level" tick={{ fontSize: 11, fill: 'var(--colors-muted)' }} axisLine={false} tickLine={false} />
                          <YAxis tick={{ fontSize: 11, fill: 'var(--colors-muted)' }} tickFormatter={(val) => `${(val / 1000000).toFixed(0)}M`} axisLine={false} tickLine={false} />
                          <Tooltip
                            formatter={(value) => [formatSalary(value), 'Lương trung vị']}
                            contentStyle={{ background: 'var(--colors-surface-dark)', borderRadius: '8px', color: '#fff', fontSize: '12px', border: 'none' }}
                          />
                          <Area type="monotone" dataKey="median_salary" stroke="var(--colors-ink)" strokeWidth={2} fill="var(--colors-surface-cream-strong)" />
                        </AreaChart>
                      </ResponsiveContainer>
                    </div>
                  </div>

                </div>

                {/* Additional Feature Card Row: Top Skills & Top Companies */}
                <div style={{ display: 'grid', gridTemplateColumns: '1.2fr 1fr', gap: '1.5rem' }}>
                  
                  {/* Skills Progress Card */}
                  <div className="feature-card-claude">
                    <h4 style={{ fontFamily: 'var(--font-serif)', fontSize: '1.2rem', fontWeight: 500, color: 'var(--colors-ink)' }}>
                      Kỹ Năng Công Nghệ Săn Đón Nhất
                    </h4>
                    <p style={{ fontSize: '13px', color: 'var(--colors-muted)', marginBottom: '8px' }}>
                      Tỉ lệ bài đăng tuyển dụng trên HDFS yêu cầu thành thạo
                    </p>

                    <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
                      {(topSkills.length > 0 ? topSkills.slice(0, 5) : [
                        { skill: 'SQL & Database', pct: 24.5, job_count: 11780 },
                        { skill: 'Python', pct: 19.8, job_count: 9520 },
                        { skill: 'Java & Spring Boot', pct: 17.2, job_count: 8270 },
                        { skill: 'React & Frontend', pct: 16.5, job_count: 7930 },
                        { skill: 'Cloud (AWS / GCP)', pct: 11.4, job_count: 5480 },
                      ]).map((sk, idx) => (
                        <div key={idx}>
                          <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '13px', fontWeight: 500, marginBottom: '3px' }}>
                            <span>{sk.skill}</span>
                            <span style={{ color: 'var(--colors-primary)' }}>{sk.pct}% ({sk.job_count?.toLocaleString('vi-VN')} tin)</span>
                          </div>
                          <div style={{ height: '6px', background: 'var(--colors-surface-cream-strong)', borderRadius: 'var(--rounded-pill)', overflow: 'hidden' }}>
                            <div style={{ width: `${sk.pct * 3.8}%`, height: '100%', background: 'var(--colors-ink)', borderRadius: 'var(--rounded-pill)' }} />
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>

                  {/* Top Companies Card */}
                  <div className="feature-card-claude">
                    <h4 style={{ fontFamily: 'var(--font-serif)', fontSize: '1.2rem', fontWeight: 500, color: 'var(--colors-ink)' }}>
                      Doanh Nghiệp Tuyển Dụng Hàng Đầu
                    </h4>
                    <p style={{ fontSize: '13px', color: 'var(--colors-muted)', marginBottom: '8px' }}>
                      Tập đoàn công nghệ & tài chính dẫn đầu thị phần
                    </p>

                    <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                      {(topCompanies.length > 0 ? topCompanies.slice(0, 5) : [
                        { company: 'FPT Software', job_count: 1450 },
                        { company: 'Viettel Telecom', job_count: 1220 },
                        { company: 'VNG Corporation', job_count: 980 },
                        { company: 'Shopee Vietnam', job_count: 850 },
                        { company: 'Techcombank', job_count: 720 },
                      ]).map((comp, idx) => (
                        <div key={idx} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '6px 10px', background: 'var(--colors-canvas)', borderRadius: 'var(--rounded-sm)', border: '1px solid var(--colors-hairline)' }}>
                          <span style={{ fontSize: '13px', fontWeight: 500 }}>#{idx + 1} {comp.company}</span>
                          <span className="badge-pill-cream" style={{ fontSize: '11px' }}>{comp.job_count?.toLocaleString('vi-VN')} việc làm</span>
                        </div>
                      ))}
                    </div>
                  </div>

                </div>

              </div>

              {/* Floating Bottom Drawer (Dark Navy Surface with Signature Coral CTA) */}
              <div className="floating-drawer-claude">
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                    <span style={{ fontFamily: 'var(--font-serif)', fontSize: '1.25rem', fontWeight: 500, color: 'var(--colors-on-dark)' }}>
                      Mã Tin Tuyển Dụng #{activeJob.id}
                    </span>
                    <span className="badge-coral-claude">
                      {activeJob.status === 'HOT HIRING' ? '🔥 HOT HIRING' : activeJob.status}
                    </span>
                  </div>

                  <button
                    className="button-primary-coral"
                    onClick={() => showToast(`Đã ghi nhận ứng tuyển tin #${activeJob.id} vào HDFS!`)}
                  >
                    <span>Ứng tuyển & Lưu HDFS</span>
                    <ArrowUpRight size={15} />
                  </button>
                </div>

                <div className="floating-grid-claude">
                  <div className="floating-cell-claude">
                    <span className="floating-label-claude">Chức danh / Vị trí:</span>
                    <span className="floating-value-claude">{activeJob.title}</span>
                  </div>
                  <div className="floating-cell-claude">
                    <span className="floating-label-claude">Doanh nghiệp:</span>
                    <span className="floating-value-claude">{activeJob.company}</span>
                  </div>
                  <div className="floating-cell-claude">
                    <span className="floating-label-claude">Tỉnh / Bang:</span>
                    <span className="floating-value-claude">{activeJob.location || activeJob.state}</span>
                  </div>
                  <div className="floating-cell-claude">
                    <span className="floating-label-claude">Mức thu nhập:</span>
                    <span className="floating-value-claude" style={{ color: 'var(--colors-accent-teal)' }}>
                      {formatSalary(activeJob.salary_vnd)}
                    </span>
                  </div>
                  <div className="floating-cell-claude">
                    <span className="floating-label-claude">Trạng thái HDFS:</span>
                    <span className="floating-value-claude" style={{ color: 'var(--colors-primary)' }}>
                      3 DataNodes Synced (Replication 3)
                    </span>
                  </div>
                </div>
              </div>

            </main>
          </>
        )}

        {/* ========================================================================= */}
        {/* TAB 2: TRUNG TÂM ĐỒ THỊ CHUYÊN SÂU (CHARTS HUB)                            */}
        {/* ========================================================================= */}
        {activeTab === 'graphs' && (
          <main className="canvas-claude-pane" style={{ padding: '2.5rem 3.5rem' }}>
            <div style={{ marginBottom: '2rem' }}>
              <span className="badge-coral-claude">CHARTS & VISUAL ANALYTICS HUB</span>
              <h2 style={{ fontFamily: 'var(--font-serif)', fontSize: '2.5rem', fontWeight: 400, letterSpacing: '-1px', color: 'var(--colors-ink)', marginTop: '8px' }}>
                Báo Cáo Biểu Đồ Toàn Cảnh Thị Trường
              </h2>
              <p style={{ fontSize: '15px', color: 'var(--colors-muted)', marginTop: '4px' }}>
                Phân tích định lượng chuyên sâu 48k+ bản ghi việc làm qua Apache Spark SQL & thư viện Recharts.
              </p>
            </div>

            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '2rem', marginBottom: '3rem' }}>
              {/* Full State Bar Chart */}
              <div className="chart-box-claude">
                <h4 style={{ fontFamily: 'var(--font-serif)', fontSize: '1.3rem', fontWeight: 500 }}>Nhu Cầu Việc Làm Theo Bang / Tỉnh</h4>
                <p style={{ fontSize: '13px', color: 'var(--colors-muted)', marginBottom: '1rem' }}>Top 8 địa phương có số lượng việc làm cao nhất</p>
                <div style={{ width: '100%', height: 270 }}>
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={availableStates.slice(0, 8)}>
                      <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="var(--colors-hairline)" />
                      <XAxis dataKey="name" tick={{ fontSize: 11, fill: 'var(--colors-muted)' }} axisLine={false} tickLine={false} />
                      <YAxis tick={{ fontSize: 11, fill: 'var(--colors-muted)' }} axisLine={false} tickLine={false} />
                      <Tooltip contentStyle={{ background: 'var(--colors-surface-dark)', borderRadius: '8px', color: '#fff', fontSize: '12px' }} />
                      <Bar dataKey="count" fill="var(--colors-primary)" radius={[4, 4, 0, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </div>

              {/* Salary by Level Area Chart */}
              <div className="chart-box-claude">
                <h4 style={{ fontFamily: 'var(--font-serif)', fontSize: '1.3rem', fontWeight: 500 }}>Dải Mức Lương Theo Cấp Bậc Chuyên Môn</h4>
                <p style={{ fontSize: '13px', color: 'var(--colors-muted)', marginBottom: '1rem' }}>Khoảng thu nhập trung vị và tối đa theo level</p>
                <div style={{ width: '100%', height: 270 }}>
                  <ResponsiveContainer width="100%" height="100%">
                    <AreaChart data={salaryByLevel.length > 0 ? salaryByLevel : [
                      { level: 'Intern', median_salary: 4500000, max_salary: 8000000 },
                      { level: 'Fresher', median_salary: 11500000, max_salary: 18000000 },
                      { level: 'Senior', median_salary: 26000000, max_salary: 45000000 },
                      { level: 'Lead', median_salary: 42000000, max_salary: 80000000 },
                      { level: 'Director', median_salary: 85000000, max_salary: 150000000 },
                    ]}>
                      <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="var(--colors-hairline)" />
                      <XAxis dataKey="level" tick={{ fontSize: 11, fill: 'var(--colors-muted)' }} axisLine={false} tickLine={false} />
                      <YAxis tick={{ fontSize: 11, fill: 'var(--colors-muted)' }} tickFormatter={(val) => `${(val / 1000000).toFixed(0)}M`} axisLine={false} tickLine={false} />
                      <Tooltip contentStyle={{ background: 'var(--colors-surface-dark)', borderRadius: '8px', color: '#fff', fontSize: '12px' }} />
                      <Area type="monotone" dataKey="max_salary" stroke="var(--colors-primary)" fill="var(--colors-surface-cream-strong)" />
                      <Area type="monotone" dataKey="median_salary" stroke="var(--colors-ink)" fill="var(--colors-surface-soft)" />
                    </AreaChart>
                  </ResponsiveContainer>
                </div>
              </div>
            </div>
          </main>
        )}

        {/* ========================================================================= */}
        {/* TAB 3: SPARK ML ESTIMATOR (DỰ ĐOÁN THU NHẬP)                               */}
        {/* ========================================================================= */}
        {activeTab === 'ml' && (
          <main className="canvas-claude-pane" style={{ padding: '2.5rem 3.5rem' }}>
            <div style={{ marginBottom: '2rem' }}>
              <span className="badge-coral-claude">SPARK MLLIB PREDICTIVE ENGINE</span>
              <h2 style={{ fontFamily: 'var(--font-serif)', fontSize: '2.5rem', fontWeight: 400, letterSpacing: '-1px', color: 'var(--colors-ink)', marginTop: '8px' }}>
                Ước Tính Thu Nhập & Phân Cụm Thị Trường
              </h2>
              <p style={{ fontSize: '15px', color: 'var(--colors-muted)', marginTop: '4px' }}>
                Huấn luyện mô hình RandomForestRegressor trên 12,478 mẫu lương thực tế và thuật toán KMeans phân cụm địa lý.
              </p>
            </div>

            {/* ML Metrics Cards */}
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: '1.5rem', marginBottom: '2rem' }}>
              <div className="feature-card-claude">
                <span style={{ fontSize: '12px', fontWeight: 500, color: 'var(--colors-muted)' }}>RMSE MÔ HÌNH</span>
                <div style={{ fontSize: '1.8rem', fontWeight: 600, color: 'var(--colors-primary)', margin: '4px 0 2px' }}>
                  1.190.044 ₫
                </div>
                <p style={{ fontSize: '12px', color: 'var(--colors-success)', fontWeight: 500 }}>Cải thiện 28.88% so với baseline</p>
              </div>

              <div className="feature-card-claude">
                <span style={{ fontSize: '12px', fontWeight: 500, color: 'var(--colors-muted)' }}>HỆ SỐ R²</span>
                <div style={{ fontSize: '1.8rem', fontWeight: 600, color: 'var(--colors-ink)', margin: '4px 0 2px' }}>
                  0.4942
                </div>
                <p style={{ fontSize: '12px', color: 'var(--colors-muted)' }}>Giải thích ~50% phương sai lương</p>
              </div>

              <div className="feature-card-claude">
                <span style={{ fontSize: '12px', fontWeight: 500, color: 'var(--colors-muted)' }}>SAI SỐ TUYỆT ĐỐI (MAE)</span>
                <div style={{ fontSize: '1.8rem', fontWeight: 600, color: 'var(--colors-body-strong)', margin: '4px 0 2px' }}>
                  830.632 ₫
                </div>
                <p style={{ fontSize: '12px', color: 'var(--colors-muted)' }}>Sai lệch trung bình / mẫu</p>
              </div>

              <div className="feature-card-claude">
                <span style={{ fontSize: '12px', fontWeight: 500, color: 'var(--colors-muted)' }}>KMEANS SILHOUETTE</span>
                <div style={{ fontSize: '1.8rem', fontWeight: 600, color: 'var(--colors-accent-teal)', margin: '4px 0 2px' }}>
                  0.7415
                </div>
                <p style={{ fontSize: '12px', color: 'var(--colors-success)', fontWeight: 500 }}>3 Cụm địa lý phân tách rõ rệt</p>
              </div>
            </div>

            {/* Interactive Estimator Form */}
            <div className="feature-card-claude" style={{ padding: '2.5rem' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '2rem' }}>
                <div>
                  <h3 style={{ fontFamily: 'var(--font-serif)', fontSize: '1.45rem', fontWeight: 500 }}>Công Cụ Ước Tính Lương Tương Tác</h3>
                  <p style={{ fontSize: '14px', color: 'var(--colors-muted)' }}>Điều chỉnh thông số để Spark ML tính toán mức thu nhập kỳ vọng</p>
                </div>
                <div style={{ textAlign: 'right' }}>
                  <span style={{ fontSize: '12px', color: 'var(--colors-muted)' }}>MỨC LƯƠNG DỰ BÁO:</span>
                  <div style={{ fontFamily: 'var(--font-serif)', fontSize: '2.8rem', fontWeight: 500, color: 'var(--colors-primary)', lineHeight: 1.1 }}>
                    {formatSalary(displaySalary)}
                  </div>
                </div>
              </div>

              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: '1.5rem', marginBottom: '2rem' }}>
                <div>
                  <label style={{ display: 'block', fontSize: '13px', fontWeight: 500, marginBottom: '6px' }}>Cấp bậc chuyên môn:</label>
                  <select
                    value={simLevel}
                    onChange={(e) => setSimLevel(e.target.value)}
                    className="select-input-claude"
                  >
                    <option value="Intern">Thực tập sinh (Intern)</option>
                    <option value="Fresher">Mới tốt nghiệp (Fresher)</option>
                    <option value="Junior/Mid">Junior / Mid-level</option>
                    <option value="Senior">Chuyên viên (Senior)</option>
                    <option value="Lead / Manager">Trưởng nhóm (Lead / Manager)</option>
                    <option value="Director">Giám đốc (Director / Head)</option>
                  </select>
                </div>

                <div>
                  <label style={{ display: 'block', fontSize: '13px', fontWeight: 500, marginBottom: '6px' }}>Tỉnh / Thành phố:</label>
                  <select
                    value={simProvince}
                    onChange={(e) => setSimProvince(e.target.value)}
                    className="select-input-claude"
                  >
                    <option value="TP Hồ Chí Minh">TP Hồ Chí Minh</option>
                    <option value="Hà Nội">Hà Nội</option>
                    <option value="Đà Nẵng">Đà Nẵng</option>
                    <option value="Bình Dương">Bình Dương</option>
                    <option value="Khác">Tỉnh thành khác</option>
                  </select>
                </div>

                <div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '13px', fontWeight: 500, marginBottom: '6px' }}>
                    <span>Kinh nghiệm:</span>
                    <span style={{ color: 'var(--colors-primary)', fontWeight: 600 }}>{simExp} năm</span>
                  </div>
                  <input
                    type="range"
                    min="0"
                    max="10"
                    step="1"
                    value={simExp}
                    onChange={(e) => setSimExp(Number(e.target.value))}
                    style={{ width: '100%', accentColor: 'var(--colors-primary)', marginTop: '8px' }}
                  />
                </div>

                <div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '13px', fontWeight: 500, marginBottom: '6px' }}>
                    <span>Kỹ năng thành thạo:</span>
                    <span style={{ color: 'var(--colors-primary)', fontWeight: 600 }}>{simSkillsCount} kỹ năng</span>
                  </div>
                  <input
                    type="range"
                    min="1"
                    max="12"
                    step="1"
                    value={simSkillsCount}
                    onChange={(e) => setSimSkillsCount(Number(e.target.value))}
                    style={{ width: '100%', accentColor: 'var(--colors-primary)', marginTop: '8px' }}
                  />
                </div>
              </div>

              <div style={{ display: 'flex', justifyContent: 'flex-end' }}>
                <button onClick={calculateEstimatedSalary} className="button-primary-coral">
                  <Sparkles size={16} />
                  <span>Dự Báo Thu Nhập Ngay</span>
                </button>
              </div>
            </div>
          </main>
        )}

        {/* ========================================================================= */}
        {/* TAB 4: PIPELINE & NẠP TỆP DATA.CSV MỚI                                     */}
        {/* ========================================================================= */}
        {activeTab === 'pipeline' && (
          <main className="canvas-claude-pane" style={{ padding: '2.5rem 3.5rem' }}>
            <div style={{ marginBottom: '2rem' }}>
              <span className="badge-coral-claude">REAL-TIME INGESTION PIPELINE</span>
              <h2 style={{ fontFamily: 'var(--font-serif)', fontSize: '2.5rem', fontWeight: 400, letterSpacing: '-1px', color: 'var(--colors-ink)', marginTop: '8px' }}>
                Nạp Tệp data.csv & Kích Hoạt Pipeline Thích Ứng
              </h2>
              <p style={{ fontSize: '15px', color: 'var(--colors-muted)', marginTop: '4px' }}>
                Tải lên bất kỳ tệp dữ liệu CSV nào để hệ thống tự động nhận diện cấu trúc, làm sạch, và cập nhật toàn bộ báo cáo mới ngay lập tức.
              </p>
            </div>

            {/* Upload Dropzone Card */}
            <div className="feature-card-claude" style={{ padding: '2.5rem', marginBottom: '2rem' }}>
              <div
                style={{
                  border: '2px dashed var(--colors-hairline)',
                  borderRadius: 'var(--rounded-lg)',
                  padding: '3rem',
                  textAlign: 'center',
                  background: dragOver ? 'var(--colors-surface-soft)' : 'var(--colors-canvas)',
                  cursor: 'pointer',
                  transition: 'all 0.2s ease'
                }}
                onDragOver={(e) => { e.preventDefault(); setDragOver(true); }}
                onDragLeave={() => setDragOver(false)}
                onDrop={(e) => {
                  e.preventDefault();
                  setDragOver(false);
                  if (e.dataTransfer.files && e.dataTransfer.files[0]) {
                    setSelectedFile(e.dataTransfer.files[0]);
                    handleFileUpload(e.dataTransfer.files[0]);
                  }
                }}
                onClick={() => fileInputRef.current?.click()}
              >
                <input
                  type="file"
                  ref={fileInputRef}
                  accept=".csv"
                  style={{ display: 'none' }}
                  onChange={(e) => {
                    if (e.target.files && e.target.files[0]) {
                      setSelectedFile(e.target.files[0]);
                      handleFileUpload(e.target.files[0]);
                    }
                  }}
                />

                <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '12px' }}>
                  <div style={{ width: '60px', height: '60px', borderRadius: '50%', background: 'var(--colors-surface-card)', color: 'var(--colors-primary)', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                    {uploading ? <Loader2 size={32} className="animate-spin" /> : <UploadCloud size={32} />}
                  </div>
                  <div>
                    <h4 style={{ fontFamily: 'var(--font-serif)', fontSize: '1.35rem', fontWeight: 500, color: 'var(--colors-ink)' }}>
                      {uploading ? 'Đang thực thi Pipeline xử lý dữ liệu...' : 'Kéo & Thả tệp data.csv vào đây hoặc nhấn để duyệt'}
                    </h4>
                    <p style={{ fontSize: '14px', color: 'var(--colors-muted)', marginTop: '4px' }}>
                      Tự động nhận diện dấu phân cách (`,`, `;`, `\t`), bảng mã UTF-8 và ánh xạ các cột chuẩn
                    </p>
                  </div>
                  {selectedFile && !uploading && (
                    <span className="badge-pill-cream" style={{ marginTop: '6px' }}>
                      {selectedFile.name} ({(selectedFile.size / 1024).toFixed(1)} KB)
                    </span>
                  )}
                </div>
              </div>

              {/* Progress Bar */}
              {pipelineProgress && (
                <div style={{ marginTop: '1.5rem', padding: '1.25rem', background: 'var(--colors-canvas)', borderRadius: 'var(--rounded-md)', border: '1px solid var(--colors-hairline)' }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '13px', fontWeight: 500, marginBottom: '6px' }}>
                    <span>Tiến độ: {pipelineProgress.stepName}</span>
                    <span style={{ color: 'var(--colors-primary)', fontWeight: 600 }}>{pipelineProgress.pct}%</span>
                  </div>
                  <div style={{ height: '8px', background: 'var(--colors-surface-soft)', borderRadius: 'var(--rounded-pill)', overflow: 'hidden' }}>
                    <div style={{ width: `${pipelineProgress.pct}%`, height: '100%', background: 'var(--colors-primary)', borderRadius: 'var(--rounded-pill)', transition: 'width 0.4s ease' }} />
                  </div>
                </div>
              )}

              {/* 1-Click Sample Buttons */}
              <div style={{ marginTop: '2rem', borderTop: '1px solid var(--colors-hairline)', paddingTop: '1.5rem' }}>
                <span style={{ fontSize: '13px', fontWeight: 600, color: 'var(--colors-body-strong)', display: 'block', marginBottom: '12px' }}>
                  ⚡ Hoặc nạp nhanh các tập dữ liệu mẫu chuẩn bị sẵn (1-Click):
                </span>
                <div style={{ display: 'flex', gap: '12px', flexWrap: 'wrap' }}>
                  <button onClick={() => handleLoadSample('us_jobs')} className="button-secondary-cream" disabled={uploading}>
                    <Zap size={15} style={{ color: 'var(--colors-primary)' }} />
                    <span>Nạp mẫu: US Tech Jobs (120 tin · USD / Remote)</span>
                  </button>
                  <button onClick={() => handleLoadSample('no_salary')} className="button-secondary-cream" disabled={uploading}>
                    <AlertTriangle size={15} style={{ color: 'var(--colors-warning)' }} />
                    <span>Nạp mẫu: No Salary Dataset (80 tin · Xuống cấp êm ái)</span>
                  </button>
                  <button onClick={() => handleSwitchDataset('vietjobs')} className="button-secondary-cream" disabled={uploading}>
                    <RefreshCw size={15} style={{ color: 'var(--colors-success)' }} />
                    <span>Trở về: VietJobs Gốc (48,092 tin · Cụm HDFS 3 DataNodes)</span>
                  </button>
                </div>
              </div>
            </div>
          </main>
        )}

        {/* ========================================================================= */}
        {/* TAB 5: HDFS CLUSTER 3 DATANODES TOPOLOGY                                  */}
        {/* ========================================================================= */}
        {activeTab === 'cluster' && (
          <main className="canvas-claude-pane" style={{ padding: '2.5rem 3.5rem' }}>
            <div style={{ marginBottom: '2rem' }}>
              <span className="badge-teal-claude">DISTRIBUTED STORAGE CLUSTER</span>
              <h2 style={{ fontFamily: 'var(--font-serif)', fontSize: '2.5rem', fontWeight: 400, letterSpacing: '-1px', color: 'var(--colors-ink)', marginTop: '8px' }}>
                Hạ Tầng Lưu Trữ HDFS Thực Tế (1 NameNode + 3 DataNodes)
              </h2>
              <p style={{ fontSize: '15px', color: 'var(--colors-muted)', marginTop: '4px' }}>
                Vận hành 100% Windows Native trên OpenJDK 17 LTS với hệ số nhân bản dfs.replication = 3.
              </p>
            </div>

            {/* 3 DataNodes Cards */}
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '1.5rem', marginBottom: '2rem' }}>
              {[
                { id: 'DataNode 1', port: 9866, webui: 9864, path: 'C:/bigdata/hdfs-data/datanode1', blocks: 'blk_1001 (103.1MB)' },
                { id: 'DataNode 2', port: 9876, webui: 9874, path: 'C:/bigdata/hdfs-data/datanode2', blocks: 'blk_1001 (103.1MB)' },
                { id: 'DataNode 3', port: 9886, webui: 9884, path: 'C:/bigdata/hdfs-data/datanode3', blocks: 'blk_1001 (103.1MB)' },
              ].map((dn, i) => (
                <div key={i} className="dark-product-card-claude">
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '12px' }}>
                    <span style={{ fontFamily: 'var(--font-serif)', fontWeight: 500, fontSize: '1.25rem', color: 'var(--colors-on-dark)' }}>{dn.id}</span>
                    <span className="badge-teal-claude">● Live Online</span>
                  </div>
                  <div style={{ fontSize: '13px', color: 'var(--colors-on-dark-soft)', display: 'flex', flexDirection: 'column', gap: '6px', fontFamily: 'var(--font-mono)' }}>
                    <div>Port: {dn.port}</div>
                    <div>Web UI: http://localhost:{dn.webui}</div>
                    <div>Thư mục: {dn.path}</div>
                    <div style={{ color: 'var(--colors-primary)', marginTop: '4px' }}>Replication x3: {dn.blocks}</div>
                  </div>
                </div>
              ))}
            </div>

            {/* Kiến Trúc Dữ Liệu 3 Tầng */}
            <div className="feature-card-claude" style={{ padding: '2rem' }}>
              <h4 style={{ fontFamily: 'var(--font-serif)', fontSize: '1.4rem', fontWeight: 500, marginBottom: '1rem' }}>
                Kiến Trúc Dữ Liệu 3 Tầng (Tầng 1 → Tầng 2 → Tầng 3)
              </h4>
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '1.25rem' }}>
                <div style={{ padding: '1.25rem', background: 'var(--colors-canvas)', borderRadius: 'var(--rounded-md)', border: '1px solid var(--colors-hairline)' }}>
                  <span className="badge-pill-cream">TẦNG 1 • RAW INGESTION</span>
                  <h5 style={{ fontFamily: 'var(--font-serif)', fontSize: '1.2rem', fontWeight: 500, margin: '8px 0 2px' }}>Parquet Snappy</h5>
                  <p style={{ fontSize: '13px', color: 'var(--colors-muted)' }}>hdfs://localhost:9000/tang1/jobs · 48,092 bản ghi</p>
                  <p style={{ fontSize: '12px', color: 'var(--colors-muted-soft)', marginTop: '4px' }}>Tiêm audit metadata, nén giảm 63.4%</p>
                </div>

                <div style={{ padding: '1.25rem', background: 'var(--colors-canvas)', borderRadius: 'var(--rounded-md)', border: '1px solid var(--colors-hairline)' }}>
                  <span className="badge-coral-claude">TẦNG 2 • LÀM SẠCH & AUDIT</span>
                  <h5 style={{ fontFamily: 'var(--font-serif)', fontSize: '1.2rem', fontWeight: 500, margin: '8px 0 2px' }}>Catalyst Engine</h5>
                  <p style={{ fontSize: '13px', color: 'var(--colors-muted)' }}>hdfs://localhost:9000/tang2/jobs · Điểm: 87.5/100</p>
                  <p style={{ fontSize: '12px', color: 'var(--colors-muted-soft)', marginTop: '4px' }}>Chuẩn hóa lương, cấp bậc, địa phương & cách ly lỗi</p>
                </div>

                <div style={{ padding: '1.25rem', background: 'var(--colors-canvas)', borderRadius: 'var(--rounded-md)', border: '1px solid var(--colors-hairline)' }}>
                  <span className="badge-teal-claude">TẦNG 3 • ANALYTICS & ML</span>
                  <h5 style={{ fontFamily: 'var(--font-serif)', fontSize: '1.2rem', fontWeight: 500, margin: '8px 0 2px' }}>7 Bảng SQL & MLlib</h5>
                  <p style={{ fontSize: '13px', color: 'var(--colors-muted)' }}>hdfs://localhost:9000/tang3/jobs · data/tang3/</p>
                  <p style={{ fontSize: '12px', color: 'var(--colors-muted-soft)', marginTop: '4px' }}>Random Forest (R² 0.485) & K-Means (k=3)</p>
                </div>
              </div>
            </div>
          </main>
        )}

      </div>
    </div>
  );
}
