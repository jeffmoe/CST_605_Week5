import matplotlib
matplotlib.use('Agg')  
import matplotlib.pyplot as plt
import pandas as pd
import logging
import os
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WeatherVisualizer:
    def __init__(self, output_dir='weather_visualizations'):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        plt.style.use('default')
        
    def generate_histograms(self, df, visualization_num=None):
        if df.empty:
            logger.warning("No data to visualize")
            return
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if visualization_num:
            viz_suffix = f"_viz{visualization_num:03d}"
        else:
            viz_suffix = ""
        if len(df) == 1:
            logger.info("Only one reading available. Creating single-value visualizations.")
            self._visualize_single_reading(df, timestamp, viz_suffix)
            return
        metrics = [
            ('temperature_c', 'Temperature (°C)', 'skyblue', 'Temperature'),
            ('humidity', 'Humidity (%)', 'lightblue', 'Humidity'),
            ('wind_speed_kph', 'Wind Speed (km/h)', 'lightgreen', 'Wind Speed'),
            ('pressure_mb', 'Pressure (mb)', 'lightcoral', 'Pressure'),
            ('precip_mm', 'Precipitation (mm)', 'lightgray', 'Precipitation'),
            ('cloud_cover', 'Cloud Cover (%)', 'lightyellow', 'Cloud Cover'),
            ('feelslike_c', 'Feels Like (°C)', 'lightsalmon', 'Feels Like'),
            ('uv_index', 'UV Index', 'lightpink', 'UV Index'),
            ('visibility_km', 'Visibility (km)', 'lightcyan', 'Visibility')
        ]
        available_metrics = []
        for m, label, color, title in metrics:
            if m in df.columns:
                data = df[m].dropna()
                if len(data) > 1 and data.nunique() > 1:
                    available_metrics.append((m, label, color, title))
                elif len(data) == 1:
                    logger.info(f"Metric '{m}' has only one value: {data.iloc[0]}")
        
        if not available_metrics and len(df) > 1:
            logger.warning("No metrics with variation found for histograms")
            self._visualize_single_reading(df, timestamp, viz_suffix)
            return
        for metric, label, color, title in available_metrics:
            self._plot_histogram(df[metric].dropna(), metric, label, color, title, timestamp, viz_suffix)
        self._plot_combined_histograms(df, available_metrics[:4], timestamp, viz_suffix)
        self._plot_time_series(df, timestamp, viz_suffix)
        self._save_statistics(df, timestamp, viz_suffix)
        self._plot_trend_comparison(df, timestamp, viz_suffix)
        
        logger.info(f"Visualization {viz_suffix} saved to {self.output_dir}/")
    
    def _plot_trend_comparison(self, df, timestamp, viz_suffix):
        import glob
        stats_files = glob.glob(f"{self.output_dir}/statistics_*.csv")
        
        if len(stats_files) > 1:
            try:
                current_temp = df['temperature_c'].mean()
                current_humidity = df['humidity'].mean()
                prev_temps = []
                timestamps = []
                
                for file in sorted(stats_files)[-5:]:  
                    stats_df = pd.read_csv(file)
                    temp_row = stats_df[stats_df['metric'] == 'temperature_c']
                    if not temp_row.empty:
                        prev_temps.append(temp_row['mean'].values[0])
                        file_time = file.split('_')[-1].replace('.csv', '')
                        timestamps.append(file_time)
                
                if len(prev_temps) > 1:
                    plt.figure(figsize=(10, 6))
                    plt.plot(range(len(prev_temps)), prev_temps, 'bo-', linewidth=2, markersize=8)
                    plt.axhline(y=current_temp, color='red', linestyle='--', 
                               label=f'Current: {current_temp:.1f}°C')
                    plt.title('Temperature Trend Across Visualizations', fontsize=14, fontweight='bold')
                    plt.xlabel('Visualization Number')
                    plt.ylabel('Average Temperature (°C)')
                    plt.grid(True, alpha=0.3)
                    plt.legend()
                    
                    filename = f"{self.output_dir}/trend_comparison{timestamp}{viz_suffix}.png"
                    plt.savefig(filename, dpi=100, bbox_inches='tight')
                    plt.close()
                    logger.info(f"Saved trend comparison: {filename}")
                    
            except Exception as e:
                logger.error(f"Error plotting trend comparison: {e}")
    
    def _visualize_single_reading(self, df, timestamp, viz_suffix):
        plt.figure(figsize=(12, 8))
        plt.axis('off')
        row = df.iloc[0]
        text = f"Current Weather in Minneapolis\n"
        text += f"{'='*35}\n\n"
        text += f"Reading #{row.get('reading_id', 'N/A')}\n"
        text += f"Collection Time: {row.get('collection_time', 'N/A')}\n"
        text += f"{'-'*35}\n"
        text += f"Temperature: {row.get('temperature_c', 'N/A')}°C\n"
        text += f"Feels Like: {row.get('feelslike_c', 'N/A')}°C\n"
        text += f"Weather: {row.get('weather_description', 'N/A')}\n"
        text += f"Humidity: {row.get('humidity', 'N/A')}%\n"
        text += f"Wind: {row.get('wind_speed_kph', 'N/A')} km/h {row.get('wind_dir', '')}\n"
        text += f"Pressure: {row.get('pressure_mb', 'N/A')} mb\n"
        text += f"Precipitation: {row.get('precip_mm', 'N/A')} mm\n"
        text += f"Cloud Cover: {row.get('cloud_cover', 'N/A')}%\n"
        text += f"UV Index: {row.get('uv_index', 'N/A')}\n"
        text += f"Visibility: {row.get('visibility_km', 'N/A')} km\n"
        text += f"\nNote: Only one reading available.\n"
        text += f"Multiple readings needed for histograms."
        
        plt.text(0.1, 0.5, text, fontsize=12, verticalalignment='center',
                fontfamily='monospace', transform=plt.gca().transAxes)
        plt.title(f"Minneapolis Current Weather - {viz_suffix}", fontsize=16, fontweight='bold')
        
        filename = f"{self.output_dir}/current_weather_summary{timestamp}{viz_suffix}.png"
        plt.savefig(filename, dpi=100, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Saved current weather summary: {filename}")
    
    def _plot_histogram(self, data, metric, label, color, title, timestamp, viz_suffix):
        try:
            if len(data) == 0:
                return
                
            plt.figure(figsize=(10, 6))
            n_bins = min(10, len(data))
            
            n, bins, patches = plt.hist(data, bins=n_bins, edgecolor='black', 
                                        color=color, alpha=0.7, rwidth=0.95)
            mean_val = data.mean()
            median_val = data.median()
            std_val = data.std()
            
            plt.axvline(mean_val, color='red', linestyle='dashed', linewidth=2, 
                       label=f'Mean: {mean_val:.2f}')
            plt.axvline(median_val, color='green', linestyle='dashed', linewidth=2, 
                       label=f'Median: {median_val:.2f}')
            if std_val > 0:
                plt.axvline(mean_val + std_val, color='orange', linestyle='dotted', linewidth=1.5,
                           label=f'+1 Std: {mean_val + std_val:.2f}')
                plt.axvline(mean_val - std_val, color='orange', linestyle='dotted', linewidth=1.5,
                           label=f'-1 Std: {mean_val - std_val:.2f}')
            
            plt.title(f'Distribution of {title}\nMinneapolis Weather ({len(data)} readings)', 
                     fontsize=14, fontweight='bold')
            plt.xlabel(label, fontsize=12)
            plt.ylabel('Frequency', fontsize=12)
            plt.legend(loc='best', fontsize=9)
            plt.grid(True, alpha=0.3, linestyle='--')
            if len(n) <= 15:
                for i in range(len(n)):
                    if n[i] > 0:
                        plt.text(bins[i] + (bins[i+1]-bins[i])/2, n[i], 
                                f'{int(n[i])}', ha='center', va='bottom', fontsize=9)
            
            plt.tight_layout()
            filename = f"{self.output_dir}/histogram_{metric}{timestamp}{viz_suffix}.png"
            plt.savefig(filename, dpi=100, bbox_inches='tight')
            plt.close()
            
            logger.info(f"Saved histogram: {filename}")
            
        except Exception as e:
            logger.error(f"Error plotting {metric}: {e}")
    
    def _plot_combined_histograms(self, df, metrics, timestamp, viz_suffix):
        try:
            if not metrics:
                return
            
            n_metrics = len(metrics)
            n_rows = (n_metrics + 1) // 2
            n_cols = 2
            
            fig, axes = plt.subplots(n_rows, n_cols, figsize=(14, 5 * n_rows))
            if n_rows == 1:
                axes = axes.flatten() if n_cols > 1 else [axes]
            else:
                axes = axes.flatten()
            for idx, (metric, label, color, title) in enumerate(metrics):
                data = df[metric].dropna()
                
                if len(data) > 0:
                    n_bins = min(8, len(data))
                    axes[idx].hist(data, bins=n_bins, edgecolor='black', 
                                  alpha=0.7, color=color, rwidth=0.9)
                    axes[idx].axvline(data.mean(), color='red', linestyle='dashed', 
                                     linewidth=2, label=f'Mean: {data.mean():.1f}')
                    axes[idx].set_title(f'{title}', fontsize=12, fontweight='bold')
                    axes[idx].set_xlabel(label, fontsize=10)
                    axes[idx].set_ylabel('Frequency', fontsize=10)
                    axes[idx].legend(fontsize=8)
                    axes[idx].grid(True, alpha=0.3, linestyle='--')
            for idx in range(len(metrics), len(axes)):
                axes[idx].set_visible(False)
            
            plt.suptitle(f'Minneapolis Weather Summary ({len(df)} readings) - {viz_suffix}', 
                        fontsize=16, fontweight='bold', y=1.02)
            plt.tight_layout()
            
            filename = f"{self.output_dir}/combined_histograms{timestamp}{viz_suffix}.png"
            plt.savefig(filename, dpi=100, bbox_inches='tight')
            plt.close()
            
            logger.info(f"Saved combined histograms: {filename}")
            
        except Exception as e:
            logger.error(f"Error plotting combined histograms: {e}")
    
    def _plot_time_series(self, df, timestamp, viz_suffix):
        if len(df) < 2 or 'collection_time' not in df.columns:
            return
        
        try:
            df_sorted = df.sort_values('collection_time')
            time_metrics = [
                ('temperature_c', 'Temperature (°C)', 'red'),
                ('humidity', 'Humidity (%)', 'blue'),
                ('pressure_mb', 'Pressure (mb)', 'green')
            ]
            
            fig, axes = plt.subplots(len(time_metrics), 1, figsize=(12, 10))
            
            for idx, (metric, label, color) in enumerate(time_metrics):
                if metric in df_sorted.columns:
                    axes[idx].plot(range(len(df_sorted)), df_sorted[metric], 
                                 'o-', color=color, linewidth=2, markersize=6)
                    axes[idx].set_title(f'{label} Over Time', fontsize=12)
                    axes[idx].set_ylabel(label, fontsize=10)
                    axes[idx].grid(True, alpha=0.3)
                    axes[idx].set_xlabel('Reading Number')
                    axes[idx].set_xticks(range(len(df_sorted)))
                    axes[idx].set_xticklabels([f"{i+1}" for i in range(len(df_sorted))])
            
            plt.suptitle(f'Minneapolis Weather - Time Series {viz_suffix}', fontsize=16, fontweight='bold')
            plt.tight_layout()
            
            filename = f"{self.output_dir}/time_series{timestamp}{viz_suffix}.png"
            plt.savefig(filename, dpi=100, bbox_inches='tight')
            plt.close()
            
            logger.info(f"Saved time series: {filename}")
            
        except Exception as e:
            logger.error(f"Error plotting time series: {e}")
    
    def _save_statistics(self, df, timestamp, viz_suffix):
        try:
            numeric_cols = df.select_dtypes(include=['number']).columns
            weather_cols = [col for col in numeric_cols if col not in ['partition', 'offset', 'reading_id']]
            
            if weather_cols:
                stats = []
                for col in weather_cols:
                    col_stats = {
                        'metric': col,
                        'count': df[col].count(),
                        'mean': df[col].mean(),
                        'std': df[col].std(),
                        'min': df[col].min(),
                        '25%': df[col].quantile(0.25),
                        '50%': df[col].median(),
                        '75%': df[col].quantile(0.75),
                        'max': df[col].max()
                    }
                    stats.append(col_stats)
                
                stats_df = pd.DataFrame(stats)
                filename = f"{self.output_dir}/statistics{timestamp}{viz_suffix}.csv"
                stats_df.to_csv(filename, index=False)
                logger.info(f"Saved statistics: {filename}")
                
                data_filename = f"{self.output_dir}/weather_readings{timestamp}{viz_suffix}.csv"
                df.to_csv(data_filename, index=False)
                logger.info(f"Saved raw data: {data_filename}")
                
        except Exception as e:
            logger.error(f"Error saving statistics: {e}")