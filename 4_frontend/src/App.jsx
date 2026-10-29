import React, { useState, useEffect, useRef, useMemo } from 'react';
import { animate, stagger } from 'animejs';
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  AreaChart,
  Area,
  CartesianGrid,
  LineChart,
  Line,
  PieChart,
  Pie,
  Cell
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
  ArrowUp,
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

function ChartTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null;
  return (
    <div className="chart-tooltip">
      <div className="chart-tooltip-label">{label}</div>
      {payload.map((entry, index) => (
        <div className="chart-tooltip-row" key={`${entry.dataKey || entry.name}-${index}`}>
          <span className="chart-tooltip-key">
            <span className="chart-tooltip-swatch" style={{ background: entry.color || 'var(--orange)' }} />
            {entry.name || entry.dataKey}
          </span>
          <strong>{typeof entry.value === 'number' ? entry.value.toLocaleString('vi-VN') : entry.value}</strong>
        </div>
      ))}
    </div>
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
  const [hotJobsByGeo, setHotJobsByGeo] = useState([]);
  const [salaryByLevel, setSalaryByLevel] = useState([]);
  const [geoData, setGeoData] = useState([]);
  const [requirements, setRequirements] = useState([]);
  const [topSkills, setTopSkills] = useState([]);
  const [topCompanies, setTopCompanies] = useState([]);
  const [quality, setQuality] = useState(null);
  const [capabilities, setCapabilities] = useState(null);
  const [market, setMarket] = useState(null);
  const [mlMetrics, setMlMetrics] = useState(null);
  const [yearDistribution, setYearDistribution] = useState([]);
  const [monthDistribution, setMonthDistribution] = useState([]);
  const [clusterHealth, setClusterHealth] = useState(null);
  const [loading, setLoading] = useState(true);

  // Real-time Search & State Explorer
  const jobsScrollRef = useRef(null);
  const [jobsScrollPercent, setJobsScrollPercent] = useState(0);

  const handleJobsScroll = (e) => {
    const { scrollTop, scrollHeight, clientHeight } = e.currentTarget;
    const total = scrollHeight - clientHeight;
    if (total > 0) {
      setJobsScrollPercent(Math.min(100, Math.round((scrollTop / total) * 100)));
    } else {
      setJobsScrollPercent(0);
    }
  };

  const scrollToTopJobs = () => {
    if (jobsScrollRef.current) {
      jobsScrollRef.current.scrollTo({ top: 0, behavior: 'smooth' });
    }
  };


  const [jobsList, setJobsList] = useState([]);
  const [selectedJob, setSelectedJob] = useState(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedState, setSelectedState] = useState('ALL');
  const [selectedYear, setSelectedYear] = useState('ALL');
  const [selectedMonth, setSelectedMonth] = useState('ALL');
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
    fetch(`${API_BASE}/health`)
      .then((response) => response.ok ? response.json() : null)
      .then(setClusterHealth)
      .catch(() => setClusterHealth(null));
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
        resHotJobsByGeo,
        resSalary,
        resGeo,
        resReq,
        resSkills,
        resComps,
        resQuality,
        resCaps,
        resMarket,
        resMl,
        resYears,
        resMonths,
        resJobs
      ] = await Promise.all([
        fetch(`${API_BASE}/datasets`).then(r => r.json()).catch(() => []),
        fetch(`${API_BASE}/datasets/${datasetId}/insights/overview`).then(r => r.json()).catch(() => []),
        fetch(`${API_BASE}/datasets/${datasetId}/insights/hot_jobs`).then(r => r.json()).catch(() => []),
        fetch(`${API_BASE}/datasets/${datasetId}/insights/hot_jobs_by_geo`).then(r => r.json()).catch(() => []),
        fetch(`${API_BASE}/datasets/${datasetId}/insights/salary_by_level`).then(r => r.json()).catch(() => []),
        fetch(`${API_BASE}/datasets/${datasetId}/insights/geo_distribution`).then(r => r.json()).catch(() => []),
        fetch(`${API_BASE}/datasets/${datasetId}/insights/requirements`).then(r => r.json()).catch(() => []),
        fetch(`${API_BASE}/datasets/${datasetId}/insights/top_skills`).then(r => r.json()).catch(() => []),
        fetch(`${API_BASE}/datasets/${datasetId}/insights/top_companies`).then(r => r.json()).catch(() => []),
        fetch(`${API_BASE}/datasets/${datasetId}/quality`).then(r => r.json()).catch(() => null),
        fetch(`${API_BASE}/datasets/${datasetId}/capabilities`).then(r => r.json()).catch(() => null),
        fetch(`${API_BASE}/datasets/${datasetId}/market`).then(r => r.json()).catch(() => null),
        fetch(`${API_BASE}/datasets/${datasetId}/insights/ml_metrics`).then(r => r.json()).catch(() => null),
        fetch(`${API_BASE}/datasets/${datasetId}/insights/year_distribution`).then(r => r.json()).catch(() => []),
        fetch(`${API_BASE}/datasets/${datasetId}/insights/month_distribution`).then(r => r.json()).catch(() => []),
        fetch(`${API_BASE}/datasets/${datasetId}/jobs?limit=80`).then(r => r.json()).catch(() => ({ jobs: [], state_counts: {} })),
      ]);

      if (Array.isArray(resDatasets)) setDatasetsList(resDatasets);
      if (resOverview && resOverview.length > 0) setOverview(resOverview[0]);
      setHotJobs(resHotJobs || []);
      setHotJobsByGeo(Array.isArray(resHotJobsByGeo) ? resHotJobsByGeo : []);
      setSalaryByLevel(resSalary || []);
      setGeoData(resGeo || []);
      setRequirements(resReq || []);
      setTopSkills(Array.isArray(resSkills) ? resSkills : []);
      setTopCompanies(resComps || []);
      setQuality(resQuality);
      setCapabilities(resCaps);
      setMarket(resMarket);
      setMlMetrics(resMl);
      setYearDistribution(resYears || []);
      setMonthDistribution(Array.isArray(resMonths) ? resMonths : []);

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
    setSelectedYear('ALL');
    setSelectedMonth('ALL');
    setSearchQuery('');
    await fetchInitialData(datasetId);
    showToast(`Đã chuyển sang tập dữ liệu: ${datasetId.toUpperCase()}`);
  };

  // Load the complete set for a selected province. The overview count is
  // computed from all rows, while the initial dashboard list is intentionally
  // capped for the hot-jobs view.
  const handleStateChange = async (state) => {
    setSelectedState(state);
    setSearchQuery('');
    setFilterMode(state === 'ALL' ? 'hot' : 'all');

    try {
      const params = new URLSearchParams({ limit: state === 'ALL' ? '80' : '1000' });
      if (state !== 'ALL') params.set('state', state);
      if (selectedYear !== 'ALL') params.set('year', selectedYear);
      if (selectedMonth !== 'ALL') params.set('month', selectedMonth);
      const response = await fetch(`${API_BASE}/datasets/${selectedDataset}/jobs?${params.toString()}`);
      if (!response.ok) throw new Error(`Jobs request failed: ${response.status}`);
      const result = await response.json();
      const loadedJobs = result?.jobs || [];
      setJobsList(loadedJobs);
      setSelectedJob(loadedJobs[0] || null);
    } catch (err) {
      console.error('Failed to load jobs for selected state:', err);
      showToast('Không thể tải danh sách việc làm của tỉnh đã chọn.');
      setJobsList([]);
      setSelectedJob(null);
    }
  };

  // Filter Jobs in Master List
  const filteredJobs = useMemo(() => {
    return jobsList.filter(job => {
      // 1. Filter by State
      // Province selection is already applied by the API. Do not apply a
      // second client-side string comparison because multi-location rows and
      // normalized province labels can use different representations.
      if (selectedState !== 'ALL' && jobsList.length === 0) {
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
        const hasHotSignal = Number(job.hot_score) > 0 || job.status === 'HOT HIRING';
        if (hasHotSignal && !((job.hot_score || 0) >= 80 || job.status === 'HOT HIRING')) return false;
      } else if (filterMode === 'high_salary') {
        const hasHighSalary = (job.salary_vnd && job.salary_vnd >= 25000000)
          || (job.salary || '').includes('triệu')
          || (job.salary || '').includes('$');
        if (!hasHighSalary) return false;
      }

      if (selectedYear !== 'ALL' && String(job.posted_year || '') !== String(selectedYear)) {
        return false;
      }
      if (selectedMonth !== 'ALL' && String(job.posted_month || '') !== String(selectedMonth)) {
        return false;
      }

      return true;
    });
  }, [jobsList, selectedState, searchQuery, filterMode, selectedYear, selectedMonth]);

  const availableYears = useMemo(() => {
    const years = yearDistribution
      .map(row => Number(row.year))
      .filter(year => Number.isInteger(year) && year > 1900);
    if (years.length > 0) return [...new Set(years)].sort((a, b) => b - a);
    return [...new Set(jobsList.map(job => Number(job.posted_year)).filter(year => year > 1900))]
      .sort((a, b) => b - a);
  }, [yearDistribution, jobsList]);

  const availableMonths = useMemo(() => {
    const months = monthDistribution
      .map(row => Number(row.month))
      .filter(month => Number.isInteger(month) && month >= 1 && month <= 12);
    if (months.length > 0) return [...new Set(months)].sort((a, b) => a - b);
    return [...new Set(jobsList.map(job => Number(job.posted_month)).filter(month => month >= 1 && month <= 12))]
      .sort((a, b) => a - b);
  }, [monthDistribution, jobsList]);

  const chartSalaryData = useMemo(() => salaryByLevel.map(row => ({
    ...row,
    segment: row.segment || row.level || 'Vai trò phổ thông',
    median_salary: Number(row.median_salary ?? row.median_salary_vnd ?? 0),
    max_salary: Number(row.max_salary ?? row.max_salary_vnd ?? 0),
    p25_salary: Number(row.p25_salary ?? row.p25_salary_vnd ?? 0),
    p75_salary: Number(row.p75_salary ?? row.p75_salary_vnd ?? 0)
  })).filter(row => row.median_salary > 0), [salaryByLevel]);

  const chartSkillsData = useMemo(() => {
    if (topSkills.length > 0) return topSkills;
    const counts = new Map();
    jobsList.forEach(job => (job.skills || []).forEach(skill => {
      const cleanSkill = String(skill || '').trim();
      if (cleanSkill) counts.set(cleanSkill, (counts.get(cleanSkill) || 0) + 1);
    }));
    const total = jobsList.length || 1;
    return [...counts.entries()]
      .map(([skill, frequency]) => ({
        skill,
        frequency,
        prevalence_pct: Number((frequency * 100 / total).toFixed(2))
      }))
      .sort((a, b) => b.frequency - a.frequency);
  }, [topSkills, jobsList]);

  const chartHotJobsData = useMemo(() => {
    const source = selectedState === 'ALL'
      ? hotJobsByGeo
      : hotJobsByGeo.filter(row => String(row.province || '').toLowerCase() === String(selectedState).toLowerCase());
    const grouped = new Map();
    source.forEach(row => {
      const title = row.job_title || 'Không có tiêu đề';
      const current = grouped.get(title) || { title, job_count: 0, application_count: 0 };
      current.job_count += Number(row.job_count || 0);
      current.application_count += Number(row.application_count || 0);
      grouped.set(title, current);
    });
    const hasApplications = [...grouped.values()].some(row => row.application_count > 0);
    return [...grouped.values()]
      .sort((a, b) => b.application_count - a.application_count || b.job_count - a.job_count)
      .slice(0, 10)
      .map(row => ({ ...row, metric: hasApplications ? row.application_count : row.job_count }));
  }, [hotJobsByGeo, selectedState]);
  const hasApplicationData = chartHotJobsData.some(row => row.application_count > 0);
  const hotJobsMetricName = hasApplicationData ? 'Lượt ứng tuyển' : 'Số tin đăng';

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

  const handleTimeChange = async (year, month) => {
    setSelectedYear(year);
    setSelectedMonth(month);
    const query = new URLSearchParams({ limit: '1000' });
    if (selectedState !== 'ALL') query.set('state', selectedState);
    if (year !== 'ALL') query.set('year', year);
    if (month !== 'ALL') query.set('month', month);
    try {
      const insightQuery = new URLSearchParams();
      if (year !== 'ALL') insightQuery.set('year', year);
      if (month !== 'ALL') insightQuery.set('month', month);
      const insightModules = ['overview', 'hot_jobs', 'hot_jobs_by_geo', 'salary_by_level', 'geo_distribution', 'requirements', 'top_skills', 'top_companies'];
      const [result, ...insights] = await Promise.all([
        fetch(`${API_BASE}/datasets/${selectedDataset}/jobs?${query.toString()}`).then(response => {
          if (!response.ok) throw new Error(`Jobs request failed: ${response.status}`);
          return response.json();
        }),
        ...insightModules.map(module =>
          fetch(`${API_BASE}/datasets/${selectedDataset}/insights/${module}?${insightQuery.toString()}`)
            .then(response => response.ok ? response.json() : [])
        )
      ]);
      const loadedJobs = result?.jobs || [];
      setJobsList(loadedJobs);
      setSelectedJob(loadedJobs[0] || null);
      const [filteredOverview, filteredHotJobs, filteredHotJobsByGeo, filteredSalary, filteredGeo, filteredRequirements, filteredSkills, filteredCompanies] = insights;
      if (Array.isArray(filteredOverview) && filteredOverview.length) setOverview(filteredOverview[0]);
      setHotJobs(Array.isArray(filteredHotJobs) ? filteredHotJobs : []);
      setHotJobsByGeo(Array.isArray(filteredHotJobsByGeo) ? filteredHotJobsByGeo : []);
      setSalaryByLevel(Array.isArray(filteredSalary) ? filteredSalary : []);
      setGeoData(Array.isArray(filteredGeo) ? filteredGeo : []);
      setRequirements(Array.isArray(filteredRequirements) ? filteredRequirements : []);
      setTopSkills(Array.isArray(filteredSkills) ? filteredSkills : []);
      setTopCompanies(Array.isArray(filteredCompanies) ? filteredCompanies : []);
    } catch (err) {
      console.error('Failed to load jobs for selected time:', err);
      showToast('Không thể tải dữ liệu theo thời gian đã chọn.');
    }
  };

  // Salary Formatter
  function formatSalary(amountVND) {
    if (!amountVND || amountVND <= 0) return 'Thỏa thuận / Chưa công khai';
    const currencyRates = { USD: 25410, MYR: 5500, SGD: 19000, GBP: 32000 };
    const currency = market?.currency || 'VND';
    if (currencyRates[currency]) {
      const localAmount = Math.round(amountVND / currencyRates[currency]);
      return `${currency} ${localAmount.toLocaleString('en-US')}/tháng`;
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
    setPipelineProgress({ step: 0, stepName: 'Đang xếp hàng chờ xử lý...', pct: 0 });

    const formData = new FormData();
    formData.append('file', file);

    try {
      const res = await fetch(`${API_BASE}/datasets/upload`, {
        method: 'POST',
        body: formData
      });
      const result = await res.json();
      if (!res.ok || !result.job_id) throw new Error(result.detail || 'Upload failed');

      await new Promise((resolve, reject) => {
        const events = new EventSource(`${API_BASE}/pipeline/jobs/${result.job_id}/events`);
        const handleEvent = (event) => {
          const payload = JSON.parse(event.data);
          setPipelineProgress({
            step: payload.status === 'completed' ? 5 : 1,
            stepName: payload.step || 'Đang xử lý pipeline qua HDFS và Spark...',
            pct: payload.progress || 0
          });
          if (payload.status === 'completed') {
            events.close();
            setPipelineResult(payload.result);
            setSelectedDataset(payload.result.dataset_id);
            fetchInitialData(payload.result.dataset_id).then(() => {
              setActiveTab('jobs');
              showToast(`Đã xử lý ${payload.result.records_processed?.toLocaleString('vi-VN')} bản ghi.`);
              resolve();
            }).catch(reject);
          } else if (payload.status === 'failed' || payload.status === 'cancelled') {
            events.close();
            reject(new Error(payload.error || 'Pipeline không hoàn tất'));
          }
        };
        ['queued', 'progress', 'completed', 'failed', 'cancelled'].forEach((eventName) => {
          events.addEventListener(eventName, handleEvent);
        });
        events.onerror = () => {
          events.close();
          reject(new Error('Mất kết nối theo dõi pipeline'));
        };
      });
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
        showToast(`Đã nạp thành công mẫu dữ liệu: ${sampleId.toUpperCase()}!`);
      }
    } catch (err) {
      console.error("Load sample error:", err);
    } finally {
      setUploading(false);
    }
  };

  // Real-time Salary Estimator Calculation
  const calculateEstimatedSalary = async () => {
    try {
      const response = await fetch(`${API_BASE}/datasets/${selectedDataset}/predict-salary`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          level: simLevel,
          province: simProvince,
          experience_years: Number(simExp),
          skill_count: Number(simSkillsCount)
        })
      });
      if (!response.ok) throw new Error(`Prediction failed: ${response.status}`);
      const result = await response.json();
      if (typeof result.predicted_salary_vnd !== 'number') throw new Error('Missing prediction');
      setDisplaySalary(Math.round(result.predicted_salary_vnd));
    } catch (error) {
      console.error('Salary prediction failed:', error);
      showToast('Mô hình MLlib chưa sẵn sàng cho bộ dữ liệu này.');
    }
  };

  // Active Job fallback
  const activeJob = selectedJob || filteredJobs[0] || {
    id: 'VN-10000',
    title: 'Kỹ sư Dữ liệu Cấp cao (Data Engineer)',
    company: 'Không có dữ liệu công ty',
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

  // Anime.js scroll and entrance animation for dedicated jobs window
  useEffect(() => {
    if (activeTab !== 'jobs') return;

    const container = jobsScrollRef.current;
    if (!container) return;

    // Initial stagger entrance for cards
    const cards = container.querySelectorAll('.job-card-animated');
    if (cards.length > 0) {
      animate(cards, {
        opacity: [0.15, 1],
        translateY: [14, 0],
        scale: [0.99, 1],
        delay: stagger(25, { start: 15 }),
        duration: 350,
        ease: 'outCubic',
      });
    }

    // Dynamic in/out scroll animation with IntersectionObserver
    const observer = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          if (entry.isIntersecting) {
            animate(entry.target, {
              opacity: [0.35, 1],
              translateY: [12, 0],
              scale: [0.99, 1],
              duration: 280,
              ease: 'outCubic',
            });
          }
        });
      },
      {
        root: container,
        threshold: 0.05,
      }
    );

    cards.forEach((card) => observer.observe(card));

    return () => {
      observer.disconnect();
    };
  }, [activeTab, filteredJobs, filterMode, selectedState]);

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
                {ds.name} ({ds.records.toLocaleString('vi-VN')} tin)
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
            <span style={{ width: '8px', height: '8px', borderRadius: '50%', background: clusterHealth?.status === 'healthy' ? 'var(--colors-success)' : 'var(--colors-warning)', display: 'inline-block' }}></span>
            <span>{clusterHealth?.cluster?.live_datanodes ?? '—'} DataNodes {clusterHealth?.status === 'healthy' ? 'Online' : 'Unavailable'}</span>
          </div>
        </div>
      </header>

      {/* 2. Main Fullpage Workspace */}
      <div className={`workspace-fullpage ${activeTab === 'jobs' ? 'jobs-layout' : ''}`}>


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
                    Đang tuyển
                  </button>
                  <button
                    className={`category-tab-btn ${filterMode === 'high_salary' ? 'active' : ''}`}
                    onClick={() => setFilterMode('high_salary')}
                  >
                    Lương cao
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
                    onChange={(e) => handleStateChange(e.target.value)}
                  >
                    <option value="ALL">Tất cả các Tỉnh / Thành phố / Bang</option>
                    {availableStates.map((st, idx) => (
                      <option key={idx} value={st.name}>
                        {st.name} ({st.count.toLocaleString('vi-VN')} việc làm)
                      </option>
                    ))}
                  </select>
                </div>
              </div>

              {/* Scroll progress indicator for the dedicated jobs window */}
              <div className="jobs-window-progress-track">
                <div
                  className="jobs-window-progress-bar"
                  style={{ width: `${jobsScrollPercent}%` }}
                />
              </div>

              {/* Dedicated Internal Scrollable Jobs Area */}
              <div
                ref={jobsScrollRef}
                onScroll={handleJobsScroll}
                className="jobs-cards-flow"
              >
                {filteredJobs.map((job) => {
                  const isSelected = activeJob.id === job.id;

                  // ACTIVE CARD: Claude Dark Navy Product Surface (#181715)
                  if (isSelected) {
                    return (
                      <div key={job.id} className="job-card-active-claude job-card-animated">
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
                          {job.education && String(job.education).trim() && (
                            <div className="cell-claude">
                              <span className="cell-label-claude">Học vấn:</span>
                              <span className="cell-val-claude">{job.education}</span>
                            </div>
                          )}
                        </div>

                        {/* Skills replace the placeholder recruiter row. */}
                        <div className="job-skills-row-claude">
                          <span className="job-skills-label-claude">Kỹ năng</span>
                          <div style={{ display: 'flex', flexWrap: 'wrap', gap: '6px' }}>
                          {(job.skills || []).map((sk, i) => (
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
                          {!job.skills?.length && <span className="job-skills-empty-claude">Chưa có dữ liệu kỹ năng</span>}
                          </div>
                        </div>
                      </div>
                    );
                  }

                  // UNSELECTED CARD: Claude Light Cream Card (#faf9f5 with #efe9de hover)
                  return (
                    <div
                      key={job.id}
                      className="job-card-claude job-card-animated"
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

              {/* Dedicated Window Status & Quick Navigation Footer */}
              <div className="jobs-window-footer">
                <span>Hiển thị <b>{filteredJobs.length}</b> vị trí</span>
                <button
                  type="button"
                  onClick={scrollToTopJobs}
                  className="jobs-scroll-top-btn"
                  title="Cuộn về đầu danh sách"
                >
                  <ArrowUp size={12} />
                  <span>Đầu trang</span>
                </button>
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
                    onClick={() => handleStateChange('ALL')}
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

              {/* Main Canvas Inner Flow */}
              <div className="canvas-inner-flow">

                <section className="dashboard-time-filter dashboard-time-filter-jobs" aria-label="Bộ lọc thời gian">
                  <div className="dashboard-time-copy">
                    <span className="dashboard-time-label">Lọc việc làm theo thời gian</span>
                    <small>
                      {selectedYear !== 'ALL' || selectedMonth !== 'ALL'
                        ? `Đang xem ${selectedYear !== 'ALL' ? `năm ${selectedYear}` : 'tất cả các năm'}${selectedMonth !== 'ALL' ? ` · tháng ${String(selectedMonth).padStart(2, '0')}` : ''}`
                        : 'Chọn năm, tháng hoặc xem toàn bộ dữ liệu'}
                    </small>
                  </div>
                  <label className="dashboard-time-control">
                    <span>Năm</span>
                    <select className="select-input-claude" value={selectedYear} onChange={(e) => handleTimeChange(e.target.value, selectedMonth)}>
                      <option value="ALL">Tất cả các năm</option>
                      {availableYears.map(year => <option key={year} value={year}>{year}</option>)}
                    </select>
                  </label>
                  <label className="dashboard-time-control">
                    <span>Tháng</span>
                    <select className="select-input-claude" value={selectedMonth} onChange={(e) => handleTimeChange(selectedYear, e.target.value)}>
                      <option value="ALL">Tất cả các tháng</option>
                      {availableMonths.map(month => <option key={month} value={month}>Tháng {String(month).padStart(2, '0')}</option>)}
                    </select>
                  </label>
                  {(selectedYear !== 'ALL' || selectedMonth !== 'ALL') && (
                    <button className="button-secondary-cream dashboard-time-reset" onClick={() => handleTimeChange('ALL', 'ALL')}>
                      Xóa bộ lọc
                    </button>
                  )}
                </section>

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
                            handleStateChange(e.activePayload[0].payload.name);
                          }
                        }}>
                          <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="var(--colors-hairline)" />
                          <XAxis dataKey="name" tick={{ fontSize: 11, fill: 'var(--colors-muted)' }} axisLine={false} tickLine={false} />
                          <YAxis tick={{ fontSize: 11, fill: 'var(--colors-muted)' }} axisLine={false} tickLine={false} />
                          <Tooltip
                            formatter={(value) => [`${value?.toLocaleString('vi-VN')} việc làm`, 'Số lượng tin']}
                            content={<ChartTooltip />}
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
                          Dải Mức Lương Theo Nhóm Vai Trò
                        </h4>
                        <p style={{ fontSize: '13px', color: 'var(--colors-muted)' }}>So sánh mức lương trung vị theo nhóm vai trò của dataset</p>
                      </div>
                      <span className="badge-pill-cream">{market?.currency || 'VND'}</span>
                    </div>

                    <div style={{ width: '100%', height: 230, marginTop: '8px' }}>
                      <ResponsiveContainer width="100%" height="100%">
                        <AreaChart data={(chartSalaryData.length > 0 ? chartSalaryData : [
                          { segment: 'Mới vào nghề', median_salary: 4500000 },
                          { segment: 'Đầu sự nghiệp', median_salary: 11500000 },
                          { segment: 'Chuyên môn cao', median_salary: 26000000 },
                          { segment: 'Quản lý / dẫn dắt', median_salary: 42000000 },
                          { segment: 'Điều hành', median_salary: 85000000 },
                        ])}>
                          <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="var(--colors-hairline)" />
                          <XAxis dataKey="segment" tick={{ fontSize: 11, fill: 'var(--colors-muted)' }} axisLine={false} tickLine={false} />
                          <YAxis tick={{ fontSize: 11, fill: 'var(--colors-muted)' }} tickFormatter={(val) => `${(val / 1000000).toFixed(0)}M`} axisLine={false} tickLine={false} />
                          <Tooltip
                            formatter={(value) => [formatSalary(value), 'Lương trung vị']}
                            content={<ChartTooltip />}
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
                      Kỹ Năng Được Săn Đón Nhiều Nhất
                    </h4>
                    <p style={{ fontSize: '13px', color: 'var(--colors-muted)', marginBottom: '8px' }}>
                      Tỉ lệ bài đăng tuyển dụng trên HDFS có nhắc đến kỹ năng
                    </p>

                    <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
                      {chartSkillsData.slice(0, 5).map((sk, idx) => (
                        <div key={idx}>
                          <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '13px', fontWeight: 500, marginBottom: '3px' }}>
                            <span>{sk.skill}</span>
                            <span style={{ color: 'var(--colors-primary)' }}>{Number(sk.prevalence_pct || sk.pct || 0).toLocaleString('vi-VN')}% ({Number(sk.frequency || sk.job_count || 0).toLocaleString('vi-VN')} tin)</span>
                          </div>
                          <div style={{ height: '6px', background: 'var(--colors-surface-cream-strong)', borderRadius: 'var(--rounded-pill)', overflow: 'hidden' }}>
                            <div style={{ width: `${Math.min(100, Number(sk.prevalence_pct || sk.pct || 0) * 3.8)}%`, height: '100%', background: 'var(--colors-ink)', borderRadius: 'var(--rounded-pill)' }} />
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
                      {topCompanies.slice(0, 5).map((comp, idx) => (
                        <div key={idx} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '6px 10px', background: 'var(--colors-canvas)', borderRadius: 'var(--rounded-sm)', border: '1px solid var(--colors-hairline)' }}>
                          <span style={{ fontSize: '13px', fontWeight: 500 }}>#{idx + 1} {comp.company}</span>
                          <span className="badge-pill-cream" style={{ fontSize: '11px' }}>{comp.job_count?.toLocaleString('vi-VN')} việc làm</span>
                        </div>
                      ))}
                      {topCompanies.length === 0 && (
                        <div className="empty-state">Nguồn dữ liệu không có cột công ty nên chưa thể xếp hạng doanh nghiệp.</div>
                      )}
                    </div>
                  </div>

                </div>

              </div>

              <section className="jobs-hot-chart-claude">
                <div className="jobs-hot-chart-heading">
                  <div>
                    <span className="section-kicker">NHU CẦU NỔI BẬT</span>
                    <h3>Công việc được quan tâm nhiều nhất</h3>
                    <p>
                      {hasApplicationData
                        ? 'Xếp hạng theo số lượt ứng tuyển trong tỉnh / bang và khoảng thời gian đang chọn.'
                          : 'Dataset không có cột apply; biểu đồ dùng số tin đăng đã xử lý để thể hiện nhu cầu tuyển dụng.'}
                    </p>
                  </div>
                    <span className="badge-coral-claude">{hotJobsMetricName}</span>
                </div>
                  {chartHotJobsData.length > 0 ? (
                  <ResponsiveContainer width="100%" height={330}>
                    <BarChart data={chartHotJobsData} layout="vertical" margin={{ top: 8, right: 24, left: 18, bottom: 8 }}>
                      <CartesianGrid strokeDasharray="3 3" horizontal={false} stroke="var(--colors-hairline)" />
                      <XAxis type="number" allowDecimals={false} tick={{ fontSize: 11 }} axisLine={false} tickLine={false} />
                      <YAxis type="category" dataKey="title" width={190} tick={{ fontSize: 11 }} axisLine={false} tickLine={false} />
                      <Tooltip content={<ChartTooltip />} />
                      <Bar dataKey="metric" name={hotJobsMetricName} fill="var(--colors-primary)" radius={[0, 5, 5, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                ) : (
                  <div className="chart-empty-state">
                    Không có công việc phù hợp với bộ lọc hiện tại.
                  </div>
                )}
              </section>

              {/* Floating Bottom Drawer (Dark Navy Surface with Signature Coral CTA) */}
              <div className="floating-drawer-claude">
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                    <span style={{ fontFamily: 'var(--font-serif)', fontSize: '1.25rem', fontWeight: 500, color: 'var(--colors-on-dark)' }}>
                      Mã Tin Tuyển Dụng #{activeJob.id}
                    </span>
                    <span className="badge-coral-claude">
                      {activeJob.status === 'HOT HIRING' ? 'HOT HIRING' : activeJob.status}
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
          <main className="canvas-claude-pane market-dashboard">
            <div className="market-dashboard-hero">
              <div>
                <span className="badge-coral-claude">MARKET INTELLIGENCE</span>
                <h2>{market?.country_name || 'Thị trường tuyển dụng'}</h2>
                <p>Toàn cảnh nhu cầu tuyển dụng, kỹ năng, lương và mức độ quan tâm của ứng viên trong dataset đang chọn.</p>
              </div>
              <div className="market-context">
                <span>Dataset đang phân tích</span>
                <strong>{datasetsList.find(ds => ds.id === selectedDataset)?.name || selectedDataset}</strong>
                <small>{market?.currency || 'N/A'} · Độ tin cậy nhận diện {Math.round((market?.confidence || 0) * 100)}%</small>
              </div>
            </div>

            <section className="dashboard-time-filter" aria-label="Bộ lọc thời gian">
              <div className="dashboard-time-copy">
                <span className="dashboard-time-label">Lọc biểu đồ theo thời gian</span>
                <small>
                  {selectedYear !== 'ALL' || selectedMonth !== 'ALL'
                    ? `Đang xem ${selectedYear !== 'ALL' ? `năm ${selectedYear}` : 'tất cả các năm'}${selectedMonth !== 'ALL' ? ` · tháng ${String(selectedMonth).padStart(2, '0')}` : ''}`
                    : 'Chọn năm, tháng hoặc xem toàn bộ dữ liệu'}
                </small>
              </div>
              <label className="dashboard-time-control">
                <span>Năm</span>
                <select
                  className="select-input-claude"
                  value={selectedYear}
                  onChange={(e) => handleTimeChange(e.target.value, selectedMonth)}
                >
                  <option value="ALL">Tất cả các năm</option>
                  {availableYears.map(year => <option key={year} value={year}>{year}</option>)}
                </select>
              </label>
              <label className="dashboard-time-control">
                <span>Tháng</span>
                <select
                  className="select-input-claude"
                  value={selectedMonth}
                  onChange={(e) => handleTimeChange(selectedYear, e.target.value)}
                >
                  <option value="ALL">Tất cả các tháng</option>
                  {availableMonths.map(month => (
                    <option key={month} value={month}>Tháng {String(month).padStart(2, '0')}</option>
                  ))}
                </select>
              </label>
              {(selectedYear !== 'ALL' || selectedMonth !== 'ALL') && (
                <button className="button-secondary-cream dashboard-time-reset" onClick={() => handleTimeChange('ALL', 'ALL')}>
                  Xóa bộ lọc
                </button>
              )}
            </section>

            <div className="market-kpi-grid">
              <div className="market-kpi"><span>Tổng vị trí</span><strong>{overview?.total_jobs?.toLocaleString('vi-VN') || '—'}</strong><small>Toàn bộ bản ghi sau Quality Gate</small></div>
              <div className="market-kpi"><span>Địa điểm tuyển dụng</span><strong>{overview?.total_provinces || availableStates.length || '—'}</strong><small>Thành phố, bang hoặc khu vực</small></div>
              <div className="market-kpi"><span>Phủ lương</span><strong>{overview?.salary_coverage_pct ? `${overview.salary_coverage_pct}%` : '—'}</strong><small>Bản ghi có mức lương hợp lệ</small></div>
              <div className="market-kpi"><span>Vị trí nổi bật</span><strong>{hotJobs[0]?.clean_title || hotJobs[0]?.job_title || '—'}</strong><small>{hotJobs[0]?.application_count ? `${Number(hotJobs[0].application_count).toLocaleString('vi-VN')} lượt ứng tuyển` : 'Theo số lượng tin đăng'}</small></div>
            </div>

            <section className="dashboard-section">
              <div className="section-heading"><div><span className="section-kicker">01 · NHU CẦU</span><h3>Thị trường đang tuyển gì?</h3></div><p>Xếp hạng theo quy mô tuyển dụng và số lượt ứng tuyển nếu dataset cung cấp trường tương ứng.</p></div>
              <div className="dashboard-chart-grid">
                <div className="chart-box-claude chart-box-wide">
                  <h4>Vị trí hot nhất</h4><p>Ưu tiên số lượt ứng tuyển; nếu không có dữ liệu, dùng số tin đăng.</p>
                  <ResponsiveContainer width="100%" height={300}>
                    <BarChart data={hotJobs.slice(0, 10)} layout="vertical" margin={{ left: 18, right: 24 }}>
                      <CartesianGrid strokeDasharray="3 3" horizontal={false} stroke="var(--colors-hairline)" />
                      <XAxis type="number" tick={{ fontSize: 11 }} axisLine={false} tickLine={false} />
                      <YAxis type="category" dataKey="clean_title" width={125} tick={{ fontSize: 10 }} axisLine={false} tickLine={false} />
                      <Tooltip content={<ChartTooltip />} />
                      <Bar dataKey={hotJobs.some(j => j.application_count > 0) ? 'application_count' : 'job_count'} fill="var(--colors-primary)" radius={[0, 5, 5, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
                <div className="chart-box-claude">
                  <h4>Tổng hợp kỹ năng được săn đón</h4><p>Toàn bộ kỹ năng được nhắc đến trong các tin tuyển dụng, không giới hạn ở công nghệ.</p>
                  <ResponsiveContainer width="100%" height={300}>
                    <BarChart data={chartSkillsData.slice(0, 8)} margin={{ bottom: 35 }}>
                      <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="var(--colors-hairline)" />
                      <XAxis dataKey="skill" angle={-28} textAnchor="end" interval={0} height={70} tick={{ fontSize: 10 }} axisLine={false} tickLine={false} />
                      <YAxis tick={{ fontSize: 11 }} axisLine={false} tickLine={false} />
                      <Tooltip content={<ChartTooltip />} />
                      <Bar dataKey="frequency" fill="var(--colors-accent-teal)" radius={[5, 5, 0, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                  {chartSkillsData.length === 0 && <div className="chart-empty-state">Dataset chưa có trường kỹ năng để tổng hợp.</div>}
                </div>
              </div>
            </section>

            <section className="dashboard-section">
              <div className="section-heading"><div><span className="section-kicker">02 · PHÂN BỔ</span><h3>Quy mô theo nơi và thời gian</h3></div><p>Nhấp vào địa điểm trong trang việc làm để xem danh sách tương ứng.</p></div>
              <div className="dashboard-chart-grid">
                <div className="chart-box-claude">
                  <h4>Địa điểm có nhiều cơ hội</h4><p>Top 10 thành phố, bang hoặc khu vực.</p>
                  <ResponsiveContainer width="100%" height={300}>
                    <BarChart data={availableStates.slice(0, 10)} margin={{ bottom: 35 }}>
                      <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="var(--colors-hairline)" />
                      <XAxis dataKey="name" angle={-28} textAnchor="end" interval={0} height={70} tick={{ fontSize: 10 }} axisLine={false} tickLine={false} />
                      <YAxis tick={{ fontSize: 11 }} axisLine={false} tickLine={false} />
                      <Tooltip content={<ChartTooltip />} />
                      <Bar dataKey="count" fill="var(--colors-primary)" radius={[5, 5, 0, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
                <div className="chart-box-claude">
                  <h4>Nhu cầu theo năm</h4><p>Số tin đăng và tổng lượt ứng tuyển theo năm đăng.</p>
                  <ResponsiveContainer width="100%" height={300}>
                    <LineChart data={yearDistribution} margin={{ right: 20 }}>
                      <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="var(--colors-hairline)" />
                      <XAxis dataKey="year" tick={{ fontSize: 11 }} axisLine={false} tickLine={false} />
                      <YAxis tick={{ fontSize: 11 }} axisLine={false} tickLine={false} />
                      <Tooltip content={<ChartTooltip />} />
                      <Line type="monotone" dataKey="job_count" name="Tin đăng" stroke="var(--colors-primary)" strokeWidth={3} dot={{ r: 3 }} />
                      <Line type="monotone" dataKey="application_count" name="Lượt ứng tuyển" stroke="var(--colors-accent-teal)" strokeWidth={2} dot={{ r: 3 }} />
                    </LineChart>
                  </ResponsiveContainer>
                  <div className="year-summary-table">
                    <div className="year-summary-row year-summary-head"><span>Năm</span><span>Tin đăng</span><span>Lượt ứng tuyển</span></div>
                    {yearDistribution.map((row) => (
                      <div className="year-summary-row" key={row.year}>
                        <span>{row.year || 'Không rõ'}</span>
                        <span>{Number(row.job_count || 0).toLocaleString('vi-VN')}</span>
                        <span>{Number(row.application_count || 0).toLocaleString('vi-VN')}</span>
                      </div>
                    ))}
                    {yearDistribution.length === 0 && <div className="year-summary-empty">Dataset chưa có ngày đăng để tổng hợp theo năm.</div>}
                  </div>
                  <div className="month-chart-block">
                    <h5>Phân bổ theo tháng</h5>
                    <ResponsiveContainer width="100%" height={150}>
                      <BarChart data={monthDistribution} margin={{ top: 8, right: 8, left: -20, bottom: 0 }}>
                        <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="var(--colors-hairline)" />
                        <XAxis dataKey="month" tickFormatter={value => `T${value}`} tick={{ fontSize: 10 }} axisLine={false} tickLine={false} />
                        <YAxis tick={{ fontSize: 10 }} axisLine={false} tickLine={false} />
                        <Tooltip content={<ChartTooltip />} />
                        <Bar dataKey="job_count" name="Tin đăng" fill="var(--colors-primary)" radius={[4, 4, 0, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                    {monthDistribution.length === 0 && <div className="chart-empty-state">Dataset chưa có tháng đăng hợp lệ.</div>}
                  </div>
                </div>
              </div>
            </section>

            <section className="dashboard-section">
              <div className="section-heading"><div><span className="section-kicker">03 · GIÁ TRỊ</span><h3>Lương và cấp bậc</h3></div><p>So sánh trung vị, biên độ thu nhập và tỷ trọng từng cấp bậc.</p></div>
              <div className="dashboard-chart-grid">
                <div className="chart-box-claude chart-box-wide">
                  <h4>Dải lương theo nhóm vai trò</h4>
                  <p>Nhóm vai trò được suy luận từ tiêu đề, dùng được cho nhiều ngành và không ép một hệ thống cấp bậc cố định.</p>
                  <ResponsiveContainer width="100%" height={300}>
                    <AreaChart data={chartSalaryData}>
                      <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="var(--colors-hairline)" />
                      <XAxis dataKey="segment" tick={{ fontSize: 10 }} axisLine={false} tickLine={false} />
                      <YAxis tickFormatter={(val) => `${(val / 1000000).toFixed(0)}M`} tick={{ fontSize: 11 }} axisLine={false} tickLine={false} />
                      <Tooltip content={<ChartTooltip />} />
                      <Area type="monotone" dataKey="max_salary" name="Cao nhất" stroke="var(--colors-primary)" fill="var(--colors-surface-cream-strong)" />
                      <Area type="monotone" dataKey="median_salary" name="Trung vị" stroke="var(--colors-ink)" fill="var(--colors-surface-soft)" />
                    </AreaChart>
                  </ResponsiveContainer>
                  {chartSalaryData.length === 0 && <div className="chart-empty-state">Dataset chưa có đủ mức lương hợp lệ để so sánh.</div>}
                </div>
                <div className="chart-box-claude">
                  <h4>Phân bổ kinh nghiệm</h4><p>Số lượng tin theo yêu cầu kinh nghiệm.</p>
                  <ResponsiveContainer width="100%" height={300}>
                    <BarChart data={requirements.slice(0, 8)}>
                      <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="var(--colors-hairline)" />
                      <XAxis dataKey="exp_group" tick={{ fontSize: 10 }} angle={-25} textAnchor="end" height={65} axisLine={false} tickLine={false} />
                      <YAxis tick={{ fontSize: 11 }} axisLine={false} tickLine={false} />
                      <Tooltip content={<ChartTooltip />} />
                      <Bar dataKey="job_count" fill="var(--colors-accent-teal)" radius={[5, 5, 0, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </div>
            </section>
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
                  Hoặc nạp nhanh các tập dữ liệu mẫu chuẩn bị sẵn (1-Click):
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

        <nav className="page-dots" aria-label="Điều hướng các trang dashboard">
          {[
            { id: 'jobs', label: 'Khám phá việc làm' },
            { id: 'graphs', label: 'Phân tích thị trường' },
            { id: 'ml', label: 'Dự báo thu nhập' },
            { id: 'pipeline', label: 'Nạp dữ liệu' },
            { id: 'cluster', label: 'Cụm HDFS' },
          ].map((page) => (
            <button
              key={page.id}
              className={`page-dot ${activeTab === page.id ? 'active' : ''}`}
              onClick={() => setActiveTab(page.id)}
              aria-label={page.label}
              aria-current={activeTab === page.id ? 'page' : undefined}
              title={page.label}
            />
          ))}
        </nav>

      </div>
    </div>
  );
}
