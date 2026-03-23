"""Plotly chart helpers with consistent styling."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

PLOT_TEMPLATE = "plotly_white"
COLOR_SEQUENCE = ["#1B5E7A", "#4C9F70", "#E07A5F", "#3D405B", "#E9C46A"]


def style_figure(fig: go.Figure, title: str | None = None) -> go.Figure:
    """Apply consistent formatting to all figures."""

    fig.update_layout(
        template=PLOT_TEMPLATE,
        colorway=COLOR_SEQUENCE,
        margin=dict(l=20, r=20, t=60 if title else 20, b=20),
        legend_title_text="",
        hovermode="x unified",
    )
    if title:
        fig.update_layout(title=title)
    return fig


def empty_figure(message: str) -> go.Figure:
    """Return a simple placeholder figure when no data is available."""

    fig = go.Figure()
    fig.add_annotation(text=message, x=0.5, y=0.5, showarrow=False)
    fig.update_xaxes(visible=False)
    fig.update_yaxes(visible=False)
    return style_figure(fig)


def line_chart(df: pd.DataFrame, x: str, y: str, title: str, color: str | None = None) -> go.Figure:
    """Create a reusable line chart."""

    if df.empty or x not in df.columns or y not in df.columns:
        return empty_figure("No data available for this chart.")
    fig = px.line(df, x=x, y=y, color=color, markers=True)
    return style_figure(fig, title)


def bar_chart(
    df: pd.DataFrame,
    x: str,
    y: str,
    title: str,
    color: str | None = None,
    orientation: str = "v",
) -> go.Figure:
    """Create a reusable bar chart."""

    if df.empty or x not in df.columns or y not in df.columns:
        return empty_figure("No data available for this chart.")
    fig = px.bar(df, x=x, y=y, color=color, orientation=orientation)
    return style_figure(fig, title)


def histogram(df: pd.DataFrame, x: str, title: str, nbins: int = 40) -> go.Figure:
    """Create a reusable histogram."""

    if df.empty or x not in df.columns:
        return empty_figure("No data available for this chart.")
    fig = px.histogram(df, x=x, nbins=nbins)
    return style_figure(fig, title)


def box_plot(df: pd.DataFrame, y: str, title: str, color: str | None = None) -> go.Figure:
    """Create a reusable box plot."""

    if df.empty or y not in df.columns:
        return empty_figure("No data available for this chart.")
    fig = px.box(df, y=y, color=color, points="outliers")
    return style_figure(fig, title)
